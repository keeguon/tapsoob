# -*- encoding : utf-8 -*-
require 'sequel'

require 'tapsoob/data_stream'
require 'tapsoob/log'
require 'tapsoob/progress_bar'
require 'tapsoob/schema'

module Tapsoob
  class Operation
    attr_reader :database_url, :dump_path, :opts

    def initialize(database_url, dump_path = nil, opts={})
      @database_url = database_url
      @dump_path    = dump_path
      @opts         = opts
      @exiting      = false
    end

    def file_prefix
      "op"
    end

    def schema_only?
      !!opts[:schema_only]
    end

    def skip_schema?
      !!opts[:skip_schema]
    end

    def indexes_first?
      !!opts[:indexes_first]
    end

    def table_filter
      opts[:tables] || []
    end

    def exclude_tables
      opts[:exclude_tables] || []
    end

    def apply_table_filter(tables)
      return tables if table_filter.empty? && exclude_tables.empty?

      if tables.kind_of?(Hash)
        ntables = {}
        tables.each do |t, d|
          if !exclude_tables.include?(t.to_s) && (!table_filter.empty? && table_filter.include?(t.to_s))
            ntables[t] = d
          end
        end
        ntables
      else
        tables.reject { |t| exclude_tables.include?(t.to_s) }.select { |t| table_filter.include?(t.to_s) }
      end
    end

    def log
      Tapsoob.log.level = Logger::DEBUG if opts[:debug]
      Tapsoob.log
    end

    def store_session
      file = "#{file_prefix}_#{Time.now.strftime("%Y%m%d%H%M")}.dat"
      log.info "\nSaving session to #{file}..."
      File.open(file, 'w') do |f|
        f.write(JSON.generate(to_hash))
      end
    end

    def to_hash
      {
        :klass            => self.class.to_s,
        :database_url     => database_url,
        :stream_state     => stream_state,
        :completed_tables => completed_tables,
        :table_filter     => table_filter,
      }
    end

    def exiting?
      !!@exiting
    end

    def setup_signal_trap
      trap("INT") {
        puts "\nCompleting current action..."
        @exiting = true
      }

      trap("TERM") {
        puts "\nCompleting current action..."
        @exiting = true
      }
    end

    def resuming?
      opts[:resume] == true
    end

    def default_chunksize
      opts[:default_chunksize]
    end

    def completed_tables
      opts[:completed_tables] ||= []
    end

    def stream_state
      opts[:stream_state] ||= {}
    end

    def stream_state=(val)
      opts[:stream_state] = val
    end

    def db
      @db ||= Sequel.connect(database_url)
      @db.loggers << Tapsoob.log if opts[:debug]

      # Set parameters
      if @db.uri =~ /oracle/i
        @db << "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"
        @db << "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS:FF6'"
      end

      @db
    end

    def format_number(num)
      num.to_s.gsub(/(\d)(?=(\d\d\d)+(?!\d))/, "\\1,")
    end

    def catch_errors(&blk)
      begin
        blk.call
      rescue Exception => e
        raise e
      end
    end

    def self.factory(type, database_url, dump_path, opts)
      type = :resume if opts[:resume]
      klass = case type
        when :pull   then Tapsoob::Pull
        when :push   then Tapsoob::Push
        when :resume then eval(opts[:klass])
        else raise "Unknown Operation Type -> #{type}"
      end

      klass.new(database_url, dump_path, opts)
    end
  end

  class Pull < Operation
    def file_prefix
      "pull"
    end

    def to_hash
      super.merge(:remote_tables_info => remote_tables_info)
    end

    def run
      catch_errors do
        unless resuming?
          pull_schema if !skip_schema?
          pull_indexes if indexes_first? && !skip_schema?
        end
        setup_signal_trap
        pull_partial_data if resuming?
        pull_data unless schema_only?
        pull_indexes if !indexes_first? && !skip_schema?
        pull_reset_sequences
      end
    end

    def pull_schema
      log.info "Receiving schema"

      progress = ProgressBar.new('Schema', tables.size)
      tables.each do |table_name, count|
        schema_data = Tapsoob::Schema.dump_table(database_url, table_name)
        log.debug "Table: #{table_name}\n#{schema_data}\n"
        output = Tapsoob::Utils.export_schema(dump_path, table_name, schema_data)
        puts output if dump_path.nil? && output
        progress.inc(1)
      end
      progress.finish
    end

    def pull_data
      log.info "Receiving data"

      log.info "#{tables.size} tables, #{format_number(record_count)} records"

      tables.each do |table_name, count|
        progress = (opts[:progress] ? ProgressBar.new(table_name.to_s, count) : nil)
        stream   = Tapsoob::DataStream.factory(db, {
          :chunksize  => default_chunksize,
          :table_name => table_name
        }, { :debug => opts[:debug] })
        pull_data_from_table(stream, progress)
      end
    end

    def pull_partial_data
      return if stream_state == {}

      table_name = stream_state[:table_name]
      record_count = tables[table_name.to_s]
      log.info "Resuming #{table_name}, #{format_number(record_count)} records"

      progress = (opts[:progress] ? ProgressBar.new(table_name.to_s, record_count) : nil)
      stream = Tapsoob::DataStream.factory(db, stream_state)
      pull_data_from_table(stream, progress)
    end

    def pull_data_from_table(stream, progress)
      loop do
        if exiting?
          store_session
          exit 0
        end

        row_size = 0
        chunksize = stream.state[:chunksize]

        begin
          chunksize = Tapsoob::Utils.calculate_chunksize(chunksize) do |c|
            stream.state[:chunksize] = c.to_i
            encoded_data, row_size, elapsed_time = nil
            d1 = c.time_delta do
              encoded_data, row_size, elapsed_time = stream.fetch
            end
        
            data = nil
            d2 = c.time_delta do
              data = {
                :state        => stream.to_hash,
                :checksum     => Tapsoob::Utils.checksum(encoded_data).to_s,
                :encoded_data => encoded_data
              }
            end
        
            stream.fetch_data_from_database(data) do |rows|
              next if rows == {}

              if dump_path.nil?
                puts JSON.generate(rows)
              else
                Tapsoob::Utils.export_rows(dump_path, stream.table_name, rows)
              end
            end
            log.debug "row size: #{row_size}"
            stream.error = false
            self.stream_state = stream.to_hash
            
            c.idle_secs = (d1 + d2)
            
            elapsed_time
          end
        rescue Tapsoob::CorruptedData => e
          log.info "Corrupted Data Received #{e.message}, retrying..."
          stream.error = true
          next
        end

        progress.inc(row_size) if progress
        
        break if stream.complete?
      end

      progress.finish if progress
      completed_tables << stream.table_name.to_s
      self.stream_state = {}
    end

    def tables
      h = {}
      tables_info.each do |table_name, count|
        next if completed_tables.include?(table_name.to_s)
        h[table_name.to_s] = count
      end
      h
    end

    def record_count
      tables_info.values.inject(:+)
    end

    def tables_info
      opts[:tables_info] ||= fetch_tables_info
    end

    def fetch_tables_info
      tables = db.tables

      data = {}
      apply_table_filter(tables).each do |table_name|
        data[table_name] = db[table_name].count
      end
      data
    end

    def self.factory(db, state)
      if defined?(Sequel::MySQL) && Sequel::MySQL.respond_to?(:convert_invalid_date_time=)
        Sequel::MySQL.convert_invalid_date_time = :nil
      end

      if state.has_key?(:klass)
        return eval(state[:klass]).new(db, state)
      end

      if Tapsoob::Utils.single_integer_primary_key(db, state[:table_name].to_sym)
        DataStreamKeyed.new(db, state)
      else
        DataStream.new(db, state)
      end
    end

    def pull_indexes
      log.info "Receiving indexes"

      raw_idxs = Tapsoob::Schema.indexes_individual(database_url)
      idxs     = (raw_idxs && raw_idxs.length >= 2 ? JSON.parse(raw_idxs) : {})

      apply_table_filter(idxs).each do |table, indexes|
        next unless indexes.size > 0
        progress = ProgressBar.new(table, indexes.size)
        indexes.each do |idx|
          output = Tapsoob::Utils.export_indexes(dump_path, table, idx)
          puts output if dump_path.nil? && output
          progress.inc(1)
        end
        progress.finish
      end
    end

    def pull_reset_sequences
      log.info "Resetting sequences"

      output = Tapsoob::Utils.schema_bin(:reset_db_sequences, database_url)
      puts output if dump_path.nil? && output
    end
  end

  class Push < Operation
    def file_prefix
      "push"
    end

    def to_hash
      super.merge(:local_tables_info => local_tables_info)
    end

    def run
      catch_errors do
        unless resuming?
          push_schema if !skip_schema?
          push_indexes if indexes_first? && !skip_schema?
        end
        setup_signal_trap
        push_partial_data if resuming?
        push_data unless schema_only?
        push_indexes if !indexes_first? && !skip_schema?
        push_reset_sequences
      end
    end

    def push_indexes
      idxs = {}
      table_idxs = Dir.glob(File.join(dump_path, "indexes", "*.json")).map { |path| File.basename(path, '.json') }
      table_idxs.each do |table_idx|
        idxs[table_idx] = JSON.parse(File.read(File.join(dump_path, "indexes", "#{table_idx}.json")))
      end

      return unless idxs.size > 0

      log.info "Sending indexes"

      apply_table_filter(idxs).each do |table, indexes|
        next unless indexes.size > 0
        progress = ProgressBar.new(table, indexes.size)
        indexes.each do |idx|
          Tapsoob::Utils.load_indexes(database_url, idx)
          progress.inc(1)
        end
        progress.finish
      end
    end

    def push_schema
      log.info "Sending schema"

      progress = ProgressBar.new('Schema', tables.size)
      tables.each do |table, count|
        log.debug "Loading '#{table}' schema\n"
        Tapsoob::Utils.load_schema(dump_path, database_url, table)
        progress.inc(1)
      end
      progress.finish
    end

    def push_reset_sequences
      log.info "Resetting sequences"

      Tapsoob::Utils.schema_bin(:reset_db_sequences, database_url)
    end

    def push_partial_data
      return if stream_state == {}

      table_name = stream_state[:table_name]
      record_count = tables[table_name.to_s]
      log.info "Resuming #{table_name}, #{format_number(record_count)} records"
      progress = ProgressBar.new(table_name.to_s, record_count)
      stream = Tapsoob::DataStream.factory(db, stream_state)
      push_data_from_file(stream, progress)
    end

    def push_data
      log.info "Sending data"

      log.info "#{tables.size} tables, #{format_number(record_count)} records"

      tables.each do |table_name, count|
        next unless File.exists?(File.join(dump_path, "data", "#{table_name}.json")) || File.exists?(File.join(dump_path, "data", "#{table_name}.json")) && JSON.parse(File.read(File.join(dump_path, "data", "#{table_name}.json")))["data"].size == 0
        db[table_name.to_sym].truncate if @opts[:purge]
        stream = Tapsoob::DataStream.factory(db, {
          :table_name => table_name,
          :chunksize => default_chunksize
        }, {
          :"skip-duplicates" => opts[:"skip-duplicates"] || false,
          :"discard-identity" => opts[:"discard-identity"] || false,
          :purge => opts[:purge] || false,
          :debug => opts[:debug]
        })
        progress = (opts[:progress] ? ProgressBar.new(table_name.to_s, count) : nil)
        push_data_from_file(stream, progress)
      end
    end

    def push_data_from_file(stream, progress)
      loop do
        if exiting?
          store_session
          exit 0
        end

        row_size = 0
        chunksize = stream.state[:chunksize]

        begin
          chunksize = Tapsoob::Utils.calculate_chunksize(chunksize) do |c|
            stream.state[:chunksize] = c.to_i
            encoded_data, row_size, elapsed_time = nil
            d1 = c.time_delta do
              encoded_data, row_size, elapsed_time = stream.fetch({ :type => "file", :source => dump_path })
            end

            data = nil
            d2 = c.time_delta do
              data = {
                :state        => stream.to_hash,
                :checksum     => Tapsoob::Utils.checksum(encoded_data).to_s,
                :encoded_data => encoded_data
              }
            end

            stream.fetch_data_to_database(data)
            log.debug "row size: #{row_size}"
            self.stream_state = stream.to_hash

            c.idle_secs = (d1 + d2)

            elapsed_time
          end
        rescue Tapsoob::CorruptedData => e
          # retry the same data, it got corrupted somehow.
          next
        rescue Tapsoob::DuplicatePrimaryKeyError => e
          # verify the stream and retry it
          stream.verify_stream
          stream = JSON.generate({ :state => stream.to_hash })
          next
        end
        stream.state[:chunksize] = chunksize

        progress.inc(row_size) if progress

        break if stream.complete?
      end

      progress.finish if progress
      completed_tables << stream.table_name.to_s
      self.stream_state = {}
    end

    def local_tables_info
      opts[:local_tables_info] ||= fetch_local_tables_info
    end

    def tables
      h = {}
      local_tables_info.each do |table_name, count|
        next if completed_tables.include?(table_name.to_s)
        h[table_name.to_s] = count
      end
      h
    end

    def record_count
      @record_count ||= local_tables_info.values.inject(0) { |a,c| a += c }
    end

    def fetch_local_tables_info
      tables_with_counts = {}
      tbls = Dir.glob(File.join(dump_path, "schemas", "*")).map { |path| File.basename(path, ".rb") }
      tbls.each do |table|
        if File.exists?(File.join(dump_path, "data", "#{table}.json"))
          data = JSON.parse(File.read(File.join(dump_path, "data", "#{table}.json")))
          tables_with_counts[table] = data["data"].size
        else
          tables_with_counts[table] = 0
        end
      end
      apply_table_filter(tables_with_counts)
    end
  end
end
