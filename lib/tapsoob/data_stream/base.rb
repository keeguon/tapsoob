# -*- encoding : utf-8 -*-
require 'tapsoob/log'
require 'tapsoob/utils'

module Tapsoob
  module DataStream
    class Base
      DEFAULT_CHUNKSIZE = 1000

      attr_reader :db, :state, :options

      def initialize(db, state, opts = {})
        @db = db
        @state = {
          :offset          => 0,
          :avg_chunksize   => 0,
          :num_chunksize   => 0,
          :total_chunksize => 0
        }.merge(state)
        @state[:chunksize] ||= DEFAULT_CHUNKSIZE
        @options = opts
        @complete = false
      end

      def log
        Tapsoob.log.level = Logger::DEBUG if state[:debug]
        Tapsoob.log
      end

      def error=(val)
        state[:error] = val
      end

      def error
        state[:error] || false
      end

      def table_name
        state[:table_name].to_sym
      end

      def table_name_sql
        table_name
      end

      def to_hash
        state.merge(:klass => self.class.to_s)
      end

      def to_json
        JSON.generate(to_hash)
      end

      def string_columns
        @string_columns ||= Tapsoob::Utils.incorrect_blobs(db, table_name)
      end

      def table
        @table ||= db[table_name_sql]
      end

      def order_by(name=nil)
        @order_by ||= begin
          name ||= table_name
          Tapsoob::Utils.order_by(db, name)
        end
      end

      def increment(row_count)
        state[:offset] += row_count
      end

      # keep a record of the average chunksize within the first few hundred thousand records, after chunksize
      # goes below 100 or maybe if offset is > 1000
      def fetch_rows
        # Only count once on first fetch
        state[:size] ||= table.count

        ds = table.order(*order_by).limit(state[:chunksize], state[:offset])
        log.debug "DataStream::Base#fetch_rows SQL -> #{ds.sql}"
        rows = Tapsoob::Utils.format_data(db, ds.all,
          :string_columns => string_columns,
          :schema => db.schema(table_name),
          :table => table_name
        )
        update_chunksize_stats
        rows
      end

      def fetch_file(dump_path)
        # Stream NDJSON format - read line by line without loading entire file
        file_path = File.join(dump_path, "data", "#{table_name}.json")

        # Initialize state on first call
        unless state[:file_initialized]
          state[:file_initialized] = true
          state[:lines_read] = 0
          state[:total_lines] = File.foreach(file_path).count
        end

        table_name_val = nil
        header_val = nil
        types_val = nil
        data_batch = []

        # Read from current offset
        File.open(file_path, 'r') do |file|
          # Skip to current offset
          state[:lines_read].times { file.gets }

          # Read chunksize worth of lines
          state[:chunksize].times do
            break if file.eof?
            line = file.gets
            next unless line

            chunk = JSON.parse(line.strip)
            table_name_val ||= chunk["table_name"]
            header_val ||= chunk["header"]
            types_val ||= chunk["types"]
            data_batch.concat(chunk["data"]) if chunk["data"]

            state[:lines_read] += 1
          end
        end

        # Apply skip-duplicates if needed
        data_batch = data_batch.uniq if @options[:"skip-duplicates"]

        # Don't set state[:size] or state[:offset] here - they're managed separately
        # for completion tracking based on actual data rows imported
        log.debug "DataStream::Base#fetch_file: read #{data_batch.size} rows from #{state[:lines_read]} lines (total #{state[:total_lines]} lines in file)"

        rows = {
          :table_name => table_name_val,
          :header     => header_val,
          :data       => data_batch,
          :types      => types_val
        }
        update_chunksize_stats
        rows
      end

      def max_chunksize_training
        20
      end

      def update_chunksize_stats
        return if state[:num_chunksize] >= max_chunksize_training
        state[:total_chunksize] += state[:chunksize]
        state[:num_chunksize] += 1
        state[:avg_chunksize] = state[:total_chunksize] / state[:num_chunksize] rescue state[:chunksize]
      end

      def encode_rows(rows)
        Tapsoob::Utils.base64encode(Marshal.dump(rows))
      end

      def fetch(opts = {})
        opts = (opts.empty? ? { :type => "database", :source => db.uri } : opts)

        log.debug "DataStream::Base#fetch state -> #{state.inspect}"

        t1 = Time.now
        rows = (opts[:type] == "file" ? fetch_file(opts[:source]) : fetch_rows)
        encoded_data = encode_rows(rows)
        t2 = Time.now
        elapsed_time = t2 - t1

        # Only increment offset for database fetches
        # For file fetches, offset is managed by fetch_file (tracks lines read, not rows)
        if opts[:type] != "file"
          state[:offset] += (rows == {} ? 0 : rows[:data].size)
        end

        [encoded_data, (rows == {} ? 0 : rows[:data].size), elapsed_time]
      end

      def complete?
        # For file-based loading, check if we've read all lines
        if state[:file_initialized]
          result = state[:lines_read] >= state[:total_lines]
          log.debug "DataStream::Base#complete? (file) lines_read=#{state[:lines_read]} total_lines=#{state[:total_lines]} result=#{result} table=#{table_name}"
          result
        else
          # For database fetching, check offset vs size
          result = state[:offset] >= state[:size]
          log.debug "DataStream::Base#complete? (db) offset=#{state[:offset]} size=#{state[:size]} result=#{result} table=#{table_name}"
          result
        end
      end

      def fetch_data_from_database(params)
        encoded_data = params[:encoded_data]

        rows = parse_encoded_data(encoded_data, params[:checksum])

        # update local state
        state.merge!(params[:state].merge(:chunksize => state[:chunksize]))

        yield rows if block_given?
        (rows == {} ? 0 : rows[:data].size)
      end

      def fetch_data_to_database(params)
        encoded_data = params[:encoded_data]

        rows = parse_encoded_data(encoded_data, params[:checksum])

        log.debug "DataStream::Base#fetch_data_to_database: importing #{rows[:data] ? rows[:data].size : 0} rows for table #{table_name rescue 'unknown'}"
        import_rows(rows)
        (rows == {} ? 0 : rows[:data].size)
      end

      def self.parse_json(json)
        hash = JSON.parse(json).symbolize_keys
        hash[:state].symbolize_keys! if hash.has_key?(:state)
        hash
      end

      def parse_encoded_data(encoded_data, checksum)
        raise Tapsoob::CorruptedData.new("Checksum Failed") unless Tapsoob::Utils.valid_data?(encoded_data, checksum)

        begin
          return Marshal.load(Tapsoob::Utils.base64decode(encoded_data))
        rescue Object => e
          unless ENV['NO_DUMP_MARSHAL_ERRORS']
            puts "Error encountered loading data, wrote the data chunk to dump.#{Process.pid}.dat"
            File.open("dump.#{Process.pid}.dat", "w") { |f| f.write(encoded_data) }
          end
          raise e
        end
      end

      def import_rows(rows)
        columns = rows[:header]
        data    = rows[:data]

        # Only import existing columns
        if table.columns.size != columns.size
          existing_columns        = table.columns.map(&:to_s)
          additional_columns      = columns - existing_columns
          additional_columns_idxs = additional_columns.map { |c| columns.index(c) }
          additional_columns_idxs.reverse.each do |idx|
            columns.delete_at(idx)
            rows[:types].delete_at(idx)
          end
          data.each_index { |didx| additional_columns_idxs.reverse.each { |idx| data[didx].delete_at(idx) } }
        end

        # Decode blobs
        if rows.has_key?(:types) && rows[:types].include?("blob")
          blob_indices = rows[:types].each_index.select { |idx| rows[:types][idx] == "blob" }
          data.each_index do |idx|
            blob_indices.each do |bi|
              data[idx][bi] = Sequel::SQL::Blob.new(Tapsoob::Utils.base64decode(data[idx][bi])) unless data[idx][bi].nil?
            end
          end
        end

        # Parse date/datetime/time columns
        if rows.has_key?(:types)
          %w(date datetime time).each do |type|
            if rows[:types].include?(type)
              type_indices = rows[:types].each_index.select { |idx| rows[:types][idx] == type }
              data.each_index do |idx|
                type_indices.each do |ti|
                  data[idx][ti] = Sequel.send("string_to_#{type}".to_sym, data[idx][ti]) unless data[idx][ti].nil?
                end
              end
            end
          end
        end

        # Remove id column
        if @options[:"discard-identity"] && rows[:header].include?("id")
          columns = rows[:header] - ["id"]
          data    = data.map { |d| d[1..-1] }
        end

        table.import(columns, data, :commit_every => 100)
      rescue Exception => ex
        case ex.message
        when /integer out of range/ then
          raise Tapsoob::InvalidData, <<-ERROR, []
  \nDetected integer data that exceeds the maximum allowable size for an integer type.
  This generally occurs when importing from SQLite due to the fact that SQLite does
  not enforce maximum values on integer types.
          ERROR
        else raise ex
        end
      end

      def verify_stream
        state[:offset] = table.count
      end

      def self.factory(db, state, opts)
        if defined?(Sequel::MySQL) && Sequel::MySQL.respond_to?(:convert_invalid_date_time=)
          Sequel::MySQL.convert_invalid_date_time = :nil
        end

        if state.has_key?(:klass)
          return eval(state[:klass]).new(db, state, opts)
        end

        if Tapsoob::Utils.single_integer_primary_key(db, state[:table_name].to_sym)
          Tapsoob::DataStream::Keyed.new(db, state, opts)
        else
          Tapsoob::DataStream::Base.new(db, state, opts)
        end
      end
    end
  end
end
