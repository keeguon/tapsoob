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
      # Only count once on first fetch
      state[:size] ||= table.count

      ds = table.order(*order_by).limit(state[:chunksize], state[:offset])
      log.debug "DataStream#fetch_rows SQL -> #{ds.sql}"
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
      log.debug "DataStream#fetch_file: read #{data_batch.size} rows from #{state[:lines_read]} lines (total #{state[:total_lines]} lines in file)"

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

      log.debug "DataStream#fetch state -> #{state.inspect}"

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
        log.debug "DataStream#complete? (file) lines_read=#{state[:lines_read]} total_lines=#{state[:total_lines]} result=#{result} table=#{table_name}"
        result
      else
        # For database fetching, check offset vs size
        result = state[:offset] >= state[:size]
        log.debug "DataStream#complete? (db) offset=#{state[:offset]} size=#{state[:size]} result=#{result} table=#{table_name}"
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

      log.debug "DataStream#fetch_data_to_database: importing #{rows[:data] ? rows[:data].size : 0} rows for table #{table_name rescue 'unknown'}"
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
        if data.any?
          # keep a record of the last primary key value in the buffer
          state[:filter] = self.buffer.last[primary_key]
        end

        break if num >= chunksize || data.empty?
      end
    end

    def fetch_buffered(chunksize)
      load_buffer(chunksize) if buffer.size < chunksize
      rows = buffer.slice(0, chunksize)
      state[:last_fetched] = rows.any? ? rows.last[primary_key] : nil
      rows
    end

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

    # Calculate PK range for partitioning
    def self.calculate_pk_ranges(db, table_name, num_partitions)
      key = Tapsoob::Utils.order_by(db, table_name).first
      ds = db[table_name.to_sym]

      # Get total row count
      total_rows = ds.count
      return [[ds.min(key) || 0, ds.max(key) || 0]] if total_rows == 0 || num_partitions <= 1

      # Calculate target rows per partition
      rows_per_partition = (total_rows.to_f / num_partitions).ceil

      # Find PK boundaries at percentiles using OFFSET
      # This ensures even distribution of ROWS, not PK values
      ranges = []
      (0...num_partitions).each do |i|
        # Calculate row offset for this partition's start
        start_offset = i * rows_per_partition
        end_offset = [(i + 1) * rows_per_partition - 1, total_rows - 1].min

        # Get the PK value at this row offset
        start_pk = ds.order(key).limit(1, start_offset).select(key).first
        start_pk = start_pk ? start_pk[key] : (ds.min(key) || 0)

        # Get the PK value at the end offset (or max for last partition)
        if i == num_partitions - 1
          end_pk = ds.max(key) || start_pk
        else
          end_pk_row = ds.order(key).limit(1, end_offset).select(key).first
          end_pk = end_pk_row ? end_pk_row[key] : start_pk
        end

        ranges << [start_pk, end_pk]
      end

      ranges
    end
  end

  # DataStream variant for PK-based range partitioning
  class DataStreamKeyedPartition < DataStream
    def initialize(db, state, opts = {})
      super(db, state, opts)
      # :partition_range = [min_pk, max_pk] for this partition
      # :last_pk = last primary key value fetched
      @state = {
        :partition_range => nil,
        :last_pk => nil
      }.merge(@state)
    end

    def primary_key
      @primary_key ||= Tapsoob::Utils.order_by(db, table_name).first
    end

    def fetch_rows
      return {} if state[:partition_range].nil?

      # Only count once on first fetch
      state[:size] ||= table.count

      min_pk, max_pk = state[:partition_range]
      chunksize = state[:chunksize]

      # Build query with PK range filter
      key = primary_key
      last = state[:last_pk] || (min_pk - 1)

      ds = table.order(*order_by).filter do
        (Sequel.identifier(key) > last) & (Sequel.identifier(key) >= min_pk) & (Sequel.identifier(key) <= max_pk)
      end.limit(chunksize)

      data = ds.all

      # Update last_pk for next fetch
      if data.any?
        state[:last_pk] = data.last[primary_key]
      else
        # No data found in this range - mark partition as complete
        state[:last_pk] = max_pk
      end

      Tapsoob::Utils.format_data(db, data,
        :string_columns => string_columns,
        :schema => db.schema(table_name),
        :table => table_name
      )
    end

    def complete?
      return true if state[:partition_range].nil?
      min_pk, max_pk = state[:partition_range]
      # Complete when we've fetched past the max PK
      state[:last_pk] && state[:last_pk] >= max_pk
    end
  end

  # DataStream variant for interleaved chunk-based partitioning (for tables without integer PK)
  class DataStreamInterleaved < DataStream
    def initialize(db, state, opts = {})
      super(db, state, opts)
      # :worker_id = which worker this is (0-indexed)
      # :num_workers = total number of workers
      # :chunk_number = current chunk number for this worker
      @state = {
        :worker_id => 0,
        :num_workers => 1,
        :chunk_number => 0
      }.merge(@state)
    end

    def fetch_rows
      worker_id = state[:worker_id]
      num_workers = state[:num_workers]
      chunk_number = state[:chunk_number]
      chunksize = state[:chunksize]

      # Only count once on first fetch
      state[:size] ||= table.count

      # Calculate which global chunk this worker should fetch
      # Worker 0: chunks 0, num_workers, 2*num_workers, ...
      # Worker 1: chunks 1, num_workers+1, 2*num_workers+1, ...
      global_chunk_index = (chunk_number * num_workers) + worker_id
      offset = global_chunk_index * chunksize

      ds = table.order(*order_by).limit(chunksize, offset)
      log.debug "DataStreamInterleaved#fetch_rows SQL -> #{ds.sql} (worker #{worker_id}/#{num_workers}, chunk #{chunk_number})"

      rows = Tapsoob::Utils.format_data(db, ds.all,
        :string_columns => string_columns,
        :schema => db.schema(table_name),
        :table => table_name
      )

      update_chunksize_stats
      rows
    end

    def fetch(opts = {})
      opts = (opts.empty? ? { :type => "database", :source => db.uri } : opts)

      log.debug "DataStreamInterleaved#fetch state -> #{state.inspect}"

      t1 = Time.now
      rows = (opts[:type] == "file" ? fetch_file(opts[:source]) : fetch_rows)
      encoded_data = encode_rows(rows)
      t2 = Time.now
      elapsed_time = t2 - t1

      row_count = (rows == {} ? 0 : rows[:data].size)

      # Always increment chunk number to avoid infinite loops
      # Even if we got 0 rows, move to the next chunk position
      state[:chunk_number] += 1
      state[:offset] += row_count

      [encoded_data, row_count, elapsed_time]
    end

    def increment(row_count)
      # This is called by the old code path - not used in new parallel implementation
      state[:chunk_number] += 1
      state[:offset] += row_count
    end

    def complete?
      state[:offset] >= state[:size]
    end
  end

  # DataStream variant for file-based parallelized loading
  # Each worker reads a different portion of the NDJSON file
  class DataStreamFilePartition < DataStream
    def initialize(db, state, opts = {})
      super(db, state, opts)
      @state = {
        :line_range => nil,  # [start_line, end_line]
        :lines_read => 0
      }.merge(@state)

      # Initialize current_line from line_range if provided
      if @state[:line_range]
        start_line, end_line = @state[:line_range]
        @state[:current_line] = start_line
      end
    end

    def fetch_file(dump_path)
      return {} if state[:line_range].nil?

      file_path = File.join(dump_path, "data", "#{table_name}.json")
      start_line, end_line = state[:line_range]

      table_name_val = nil
      header_val = nil
      types_val = nil
      data_batch = []

      # Read lines in this worker's range
      File.open(file_path, 'r') do |file|
        # Skip to current position
        state[:current_line].times { file.gets }

        # Read up to chunksize lines, but don't exceed end_line
        lines_to_read = [state[:chunksize], end_line - state[:current_line] + 1].min
        log.debug "DataStreamFilePartition#fetch_file: current_line=#{state[:current_line]} end_line=#{end_line} lines_to_read=#{lines_to_read} chunksize=#{state[:chunksize]} table=#{table_name}"

        lines_to_read.times do
          break if file.eof? || state[:current_line] > end_line

          line = file.gets
          next unless line

          chunk = JSON.parse(line.strip)
          table_name_val ||= chunk["table_name"]
          header_val ||= chunk["header"]
          types_val ||= chunk["types"]
          data_batch.concat(chunk["data"]) if chunk["data"]

          state[:current_line] += 1
        end
      end

      log.debug "DataStreamFilePartition#fetch_file: read #{data_batch.size} rows in #{state[:current_line] - start_line} lines table=#{table_name}"

      # Apply skip-duplicates if needed
      data_batch = data_batch.uniq if @options[:"skip-duplicates"]

      state[:size] = end_line - start_line + 1
      state[:offset] = state[:current_line] - start_line

      rows = {
        :table_name => table_name_val,
        :header     => header_val,
        :data       => data_batch,
        :types      => types_val
      }

      update_chunksize_stats
      rows
    end

    def complete?
      return true if state[:line_range].nil?
      start_line, end_line = state[:line_range]
      result = state[:current_line] && state[:current_line] > end_line
      log.debug "DataStreamFilePartition#complete? current_line=#{state[:current_line]} end_line=#{end_line} result=#{result} table=#{table_name}"
      result
    end
  end
end
