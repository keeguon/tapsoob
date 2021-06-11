# -*- encoding : utf-8 -*-
require 'tapsoob/log'
require 'tapsoob/utils'

module Tapsoob
  class DataStream
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
      state[:chunksize] = fetch_chunksize
      ds = table.order(*order_by).limit(state[:chunksize], state[:offset])
      log.debug "DataStream#fetch_rows SQL -> #{ds.sql}"
      rows = Tapsoob::Utils.format_data(ds.all,
        :string_columns => string_columns,
        :schema => db.schema(table_name),
        :table => table_name
      )
      update_chunksize_stats
      rows
    end

    def fetch_file(dump_path)
      state[:chunksize] = fetch_chunksize
      ds = JSON.parse(File.read(File.join(dump_path, "data", "#{table_name}.json")))
      log.debug "DataStream#fetch_file"
      rows = {
        :table_name => ds["table_name"],
        :header     => ds["header"],
        :data       => ds["data"][state[:offset], (state[:offset] + state[:chunksize])] || [ ]
      }
      update_chunksize_stats
      rows
    end

    def max_chunksize_training
      20
    end

    def fetch_chunksize
      chunksize = state[:chunksize]
      return chunksize if state[:num_chunksize] < max_chunksize_training
      return chunksize if state[:avg_chunksize] == 0
      return chunksize if state[:error]
      state[:avg_chunksize] > chunksize ? state[:avg_chunksize] : chunksize
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

      log.debug "DataStream#fetch state -> #{state.inspect}"

      t1 = Time.now
      rows = (opts[:type] == "file" ? fetch_file(opts[:source]) : fetch_rows)
      encoded_data = encode_rows(rows)
      t2 = Time.now
      elapsed_time = t2 - t1

      if opts[:type] == "file"
        @complete = rows[:data] == [ ]
      else
        @complete = rows == { }
      end

      [encoded_data, (@complete ? 0 : rows[:data].size), elapsed_time]
    end

    def complete?
      @complete
    end

    def fetch_database
      params = fetch_from_database
      encoded_data = params[:encoded_data]
      json = params[:json]

      rows = parse_encoded_data(encoded_data, json[:checksum])

      @complete = rows == { }

      # update local state
      state.merge!(json[:state].merge(:chunksize => state[:chunksize]))

      unless @complete
        yield rows if block_given?
        state[:offset] += rows[:data].size
        rows[:data].size
      else
        0
      end
    end

    def fetch_from_database
      res = nil
      log.debug "DataStream#fetch_from_database state -> #{state.inspect}"
      state[:chunksize] = Tapsoob::Utils.calculate_chunksize(state[:chunksize]) do |c|
        state[:chunksize] = c.to_i
        encoded_data = fetch.first

        checksum = Tapsoob::Utils.checksum(encoded_data).to_s

        res = {
          :json         => { :checksum => checksum, :state => to_hash },
          :encoded_data => encoded_data
        }
      end

      res
    end

    def fetch_data_in_database(params)
      encoded_data = params[:encoded_data]

      rows = parse_encoded_data(encoded_data, params[:checksum])

      @complete = rows[:data] == [ ]

      unless @complete
        import_rows(rows)
        rows[:data].size
      else
        0
      end
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

      # Decode blobs
      if rows.has_key?(:types) && rows[:types].include?("blob")
        blob_indices = rows[:types].each_index.select { |idx| rows[:types][idx] == "blob" }
        rows[:data].each_index do |idx|
          blob_indices.each do |bi|
            rows[:data][idx][bi] = Sequel::SQL::Blob.new(Tapsoob::Utils.base64decode(rows[:data][idx][bi])) unless rows[:data][idx][bi].nil?
          end
        end
      end

      # Parse date/datetime/time columns
      if rows.has_key?(:types)
        %w(date datetime time).each do |type|
          if rows[:types].include?(type)
            type_indices = rows[:types].each_index.select { |idx| rows[:types][idx] == type }
            rows[:data].each_index do |idx|
              type_indices.each do |ti|
                rows[:data][idx][ti] = Sequel.send("string_to_#{type}".to_sym, rows[:data][idx][ti]).strftime("%Y-%m-%d %H:%M:%S")
              end
            end
          end
        end
      end

      # Remove id column
      if @options[:"discard-identity"]
        columns = rows[:header] - ["id"]
        data    = data.map { |d| d[1..-1] }
      end
      
      table.import(columns, data, :commit_every => 100)
      state[:offset] += rows[:data].size
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
        DataStreamKeyed.new(db, state, opts)
      else
        DataStream.new(db, state, opts)
      end
    end
  end

  class DataStreamKeyed < DataStream
    attr_accessor :buffer

    def initialize(db, state, opts = {})
      super(db, state, opts)
      @state = { :primary_key => order_by(state[:table_name]).first, :filter => 0 }.merge(@state)
      @state[:chunksize] ||= DEFAULT_CHUNKSIZE
      @buffer = []
    end

    def primary_key
      state[:primary_key].to_sym
    end

    def buffer_limit
      if state[:last_fetched] and state[:last_fetched] < state[:filter] and self.buffer.size == 0
        state[:last_fetched]
      else
        state[:filter]
      end
    end

    def calc_limit(chunksize)
      # we want to not fetch more than is needed while we're
      # inside sinatra but locally we can select more than
      # is strictly needed
      if defined?(Sinatra)
        (chunksize * 1.1).ceil
      else
        (chunksize * 3).ceil
      end
    end

    def load_buffer(chunksize)
      # make sure BasicObject is not polluted by subsequent requires
      Sequel::BasicObject.remove_methods!

      num = 0
      loop do
        limit = calc_limit(chunksize)
        # we have to use local variables in order for the virtual row filter to work correctly
        key = primary_key
        buf_limit = buffer_limit
        ds = table.order(*order_by).filter { key.sql_number > buf_limit }.limit(limit)
        log.debug "DataStreamKeyed#load_buffer SQL -> #{ds.sql}"
        data = ds.all
        self.buffer += data
        num += data.size
        if data.size > 0
          # keep a record of the last primary key value in the buffer
          state[:filter] = self.buffer.last[ primary_key ]
        end

        break if num >= chunksize or data.size == 0
      end
    end

    def fetch_buffered(chunksize)
      load_buffer(chunksize) if self.buffer.size < chunksize
      rows = buffer.slice(0, chunksize)
      state[:last_fetched] = if rows.size > 0
        rows.last[ primary_key ]
      else
        nil
      end
      rows
    end

    #def import_rows(rows)
    #  table.import(rows[:header], rows[:data])
    #end

    #def fetch_rows
    #  chunksize = state[:chunksize]
    #  Tapsoob::Utils.format_data(fetch_buffered(chunksize) || [],
    #    :string_columns => string_columns)
    #end

    def increment(row_count)
      # pop the rows we just successfully sent off the buffer
      @buffer.slice!(0, row_count)
    end

    def verify_stream
      key = primary_key
      ds = table.order(*order_by)
      current_filter = ds.max(key.sql_number)

      # set the current filter to the max of the primary key
      state[:filter] = current_filter
      # clear out the last_fetched value so it can restart from scratch
      state[:last_fetched] = nil

      log.debug "DataStreamKeyed#verify_stream -> state: #{state.inspect}"
    end
  end
end
