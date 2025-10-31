# -*- encoding : utf-8 -*-
require 'sequel'
require 'thread'
require 'etc'

require 'tapsoob/data_stream'
require 'tapsoob/log'
require 'tapsoob/progress'
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

    def data?
      opts[:data]
    end

    def schema?
      opts[:schema]
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
      @db ||= Sequel.connect(database_url, max_connections: parallel_workers * 2)
      @db.extension :schema_dumper
      @db.loggers << Tapsoob.log if opts[:debug]

      # Set parameters
      if @db.uri =~ /oracle/i
        @db << "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"
        @db << "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS:FF6'"
      end

      @db
    end

    def parallel?
      parallel_workers > 1
    end

    def parallel_workers
      @parallel_workers ||= [opts[:parallel].to_i, 1].max
    end

    # Auto-detect number of workers for intra-table parallelization
    def table_parallel_workers(table_name, row_count)
      # Disable intra-table parallelization when piping to STDOUT
      # (no dump_path means we're outputting JSON directly, which can't be safely parallelized)
      return 1 if dump_path.nil?

      # TEMPORARILY RE-ENABLED for debugging
      # return 1 if self.is_a?(Tapsoob::Push)

      # Minimum threshold for parallelization (100K rows by default)
      threshold = 100_000
      return 1 if row_count < threshold

      # Detect available CPU cores
      available_cpus = Etc.nprocessors rescue 4

      # Use up to 50% of CPUs for single table, max 8 workers
      max_workers = [available_cpus / 2, 8, 2].max

      # Scale based on table size
      if row_count >= 5_000_000
        max_workers
      elsif row_count >= 1_000_000
        [max_workers / 2, 2].max
      elsif row_count >= 500_000
        [max_workers / 4, 2].max
      else
        2  # Minimum 2 workers for tables over threshold
      end
    end

    # Check if table can use efficient PK-based partitioning
    def can_use_pk_partitioning?(table_name)
      Tapsoob::Utils.single_integer_primary_key(db, table_name.to_sym)
    end

    def completed_tables_mutex
      @completed_tables_mutex ||= Mutex.new
    end

    def add_completed_table(table_name)
      completed_tables_mutex.synchronize do
        completed_tables << table_name.to_s
      end
    end

    def format_number(num)
      num.to_s.gsub(/(\d)(?=(\d\d\d)+(?!\d))/, "\\1,")
    end

    def save_table_order(table_names)
      return unless dump_path

      metadata_file = File.join(dump_path, "table_order.txt")
      File.open(metadata_file, 'w') do |file|
        table_names.each { |table| file.puts(table) }
      end
    end

    def load_table_order
      return nil unless dump_path

      metadata_file = File.join(dump_path, "table_order.txt")
      return nil unless File.exist?(metadata_file)

      File.readlines(metadata_file).map(&:strip).reject(&:empty?)
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
          pull_schema if schema?
          pull_indexes if indexes_first? && schema?
        end
        setup_signal_trap
        pull_partial_data if data? && resuming?
        pull_data if data?
        pull_indexes if !indexes_first? && schema?
        pull_reset_sequences
      end
    end

    def pull_schema
      log.info "Receiving schema"

      progress = ProgressBar.new('Schema', tables.size)
      tables.each do |table_name, count|
        # Reuse existing db connection for better performance
        schema_data = Tapsoob::Schema.dump_table(db, table_name, @opts.slice(:indexes, :same_db))
        log.debug "Table: #{table_name}\n#{schema_data}\n"
        output = Tapsoob::Utils.export_schema(dump_path, table_name, schema_data)
        puts output if dump_path.nil? && output
        progress.inc(1)
      end
      progress.finish

      # Save table order for dependency-aware schema loading during push
      save_table_order(tables.keys) if dump_path
    end

    def pull_data
      log.info "Receiving data"

      log.info "#{tables.size} tables, #{format_number(record_count)} records"

      if parallel?
        pull_data_parallel
      else
        pull_data_serial
      end
    end

    def pull_data_serial
      tables.each do |table_name, count|
        # Auto-detect if we should use intra-table parallelization
        table_workers = table_parallel_workers(table_name, count)

        if table_workers > 1
          log.info "Table #{table_name}: using #{table_workers} workers for #{format_number(count)} records"
          pull_data_from_table_parallel(table_name, count, table_workers)
        else
          stream = Tapsoob::DataStream.factory(db, {
            :chunksize  => default_chunksize,
            :table_name => table_name
          }, { :debug => opts[:debug] })
          estimated_chunks = [(count.to_f / default_chunksize).ceil, 1].max
          progress = (opts[:progress] ? ProgressBar.new(table_name.to_s, estimated_chunks) : nil)
          pull_data_from_table(stream, progress)
        end
      end
    end

    def pull_data_parallel
      log.info "Using #{parallel_workers} parallel workers for table-level parallelization"

      # Reserve space for both table-level and intra-table workers
      # With 4 table workers and potentially 8 intra-table workers per table,
      # we could have many concurrent progress bars. Show up to 8 at once.
      max_visible_bars = 8
      multi_progress = opts[:progress] ? MultiProgressBar.new(max_visible_bars) : nil
      table_queue = Queue.new
      tables.each { |table_name, count| table_queue << [table_name, count] }

      workers = (1..parallel_workers).map do
        Thread.new do
          loop do
            break if table_queue.empty?

            table_name, count = table_queue.pop(true) rescue break

            # Check if this table should use intra-table parallelization
            table_workers = table_parallel_workers(table_name, count)

            if table_workers > 1
              # Large table - use intra-table parallelization
              info_msg = "Table #{table_name}: using #{table_workers} workers for #{format_number(count)} records"
              if multi_progress
                multi_progress.set_info(info_msg)
              else
                log.info info_msg
              end

              # Run intra-table parallelization, passing parent progress bar
              pull_data_from_table_parallel(table_name, count, table_workers, multi_progress)
            else
              # Small table - use single-threaded processing
              stream = Tapsoob::DataStream.factory(db, {
                :chunksize  => default_chunksize,
                :table_name => table_name
              }, { :debug => opts[:debug] })

              estimated_chunks = [(count.to_f / default_chunksize).ceil, 1].max
              progress = multi_progress ? multi_progress.create_bar(table_name.to_s, estimated_chunks) : nil

              pull_data_from_table(stream, progress)
            end
          end
        end
      end

      workers.each(&:join)
      multi_progress.stop if multi_progress
    end

    def pull_partial_data
      return if stream_state == {}

      table_name = stream_state[:table_name]
      record_count = tables[table_name.to_s]
      log.info "Resuming #{table_name}, #{format_number(record_count)} records"

      stream = Tapsoob::DataStream.factory(db, stream_state)
      chunksize = stream_state[:chunksize] || default_chunksize
      estimated_chunks = [(record_count.to_f / chunksize).ceil, 1].max
      progress = (opts[:progress] ? ProgressBar.new(table_name.to_s, estimated_chunks) : nil)
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

              # Update progress bar by 1 chunk
              progress.inc(1) if progress

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
        
        break if stream.complete?
      end

      progress.finish if progress
      add_completed_table(stream.table_name)
      self.stream_state = {}
    end

    def pull_data_from_table_parallel(table_name, row_count, num_workers, parent_progress = nil)
      # Mutex for coordinating file writes
      write_mutex = Mutex.new

      # Determine partitioning strategy
      use_pk_partitioning = can_use_pk_partitioning?(table_name)

      if use_pk_partitioning
        # PK-based partitioning for efficient range queries
        ranges = Tapsoob::DataStreamKeyed.calculate_pk_ranges(db, table_name, num_workers)
        log.debug "Table #{table_name}: using PK-based partitioning with #{ranges.size} ranges"
      else
        # Interleaved chunking for tables without integer PK
        log.debug "Table #{table_name}: using interleaved chunking with #{num_workers} workers"
      end

      # Progress tracking - create ONE shared progress bar for the entire table
      estimated_chunks = [(row_count.to_f / default_chunksize).ceil, 1].max
      shared_progress = parent_progress ? parent_progress.create_bar(table_name.to_s, estimated_chunks) : nil

      workers = (0...num_workers).map do |worker_id|
        Thread.new do
          # Create worker-specific stream
          if use_pk_partitioning
            min_pk, max_pk = ranges[worker_id]
            stream = Tapsoob::DataStreamKeyedPartition.new(db, {
              :table_name => table_name,
              :chunksize => default_chunksize,
              :partition_range => [min_pk, max_pk]
            }, { :debug => opts[:debug] })
          else
            stream = Tapsoob::DataStreamInterleaved.new(db, {
              :table_name => table_name,
              :chunksize => default_chunksize,
              :worker_id => worker_id,
              :num_workers => num_workers
            }, { :debug => opts[:debug] })
          end

          # Process data chunks
          loop do
            break if exiting? || stream.complete?

            begin
              encoded_data, row_size, elapsed_time = stream.fetch

              # Skip processing empty results
              if row_size.positive?
                data = {
                  :state => stream.to_hash,
                  :checksum => Tapsoob::Utils.checksum(encoded_data).to_s,
                  :encoded_data => encoded_data
                }

                stream.fetch_data_from_database(data) do |rows|
                  next if rows == {}

                  # Thread-safe file write
                  write_mutex.synchronize do
                    if dump_path.nil?
                      puts JSON.generate(rows)
                    else
                      Tapsoob::Utils.export_rows(dump_path, stream.table_name, rows)
                    end
                  end

                  # Update shared progress bar (thread-safe)
                  shared_progress.inc(1) if shared_progress
                end
              end

              # Check completion AFTER processing data to avoid losing the last chunk
              break if stream.complete?

            rescue Tapsoob::CorruptedData => e
              log.info "Worker #{worker_id}: Corrupted Data Received #{e.message}, retrying..."
              next
            rescue StandardError => e
              log.error "Worker #{worker_id} error: #{e.message}"
              log.error e.backtrace.join("\n")
              raise
            end
          end
        end
      end

      # Wait for all workers to complete
      workers.each(&:join)

      # Finish the shared progress bar
      shared_progress.finish if shared_progress

      add_completed_table(table_name)
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
      tables = db.send(:sort_dumped_tables, db.tables, {})

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

      # Calculate max title width for consistent alignment
      filtered_idxs = apply_table_filter(idxs).select { |table, indexes| indexes.size > 0 }
      max_title_width = filtered_idxs.keys.map { |table| "#{table} indexes".length }.max || 14

      filtered_idxs.each do |table, indexes|
        progress = ProgressBar.new("#{table} indexes", indexes.size, STDOUT, max_title_width)
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

    # Disable table-level parallelization for push operations to respect
    # foreign key dependencies. Tables must be loaded in dependency order
    # (as specified in table_order.txt manifest) to avoid FK violations.
    # Intra-table parallelization is still enabled and safe.
    def parallel?
      false
    end

    def to_hash
      super.merge(:local_tables_info => local_tables_info)
    end

    def run
      catch_errors do
        unless resuming?
          push_schema if schema?
          push_indexes if indexes_first? && schema?
        end
        setup_signal_trap
        push_partial_data if data? && resuming?
        push_data if data?
        push_indexes if !indexes_first? && schema?
        push_reset_sequences
      end
    end

    def push_indexes
      idxs = {}
      table_idxs = Dir.glob(File.join(dump_path, "indexes", "*.json")).map { |path| File.basename(path, '.json') }
      table_idxs.each do |table_idx|
        # Read NDJSON format - each line is a separate index
        index_file = File.join(dump_path, "indexes", "#{table_idx}.json")
        idxs[table_idx] = File.readlines(index_file).map { |line| JSON.parse(line.strip) }
      end

      return unless idxs.size > 0

      log.info "Sending indexes"

      # Calculate max title width for consistent alignment
      filtered_idxs = apply_table_filter(idxs).select { |table, indexes| indexes.size > 0 }
      max_title_width = filtered_idxs.keys.map { |table| "#{table} indexes".length }.max || 14

      filtered_idxs.each do |table, indexes|
        progress = ProgressBar.new("#{table} indexes", indexes.size, STDOUT, max_title_width)
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
        # Reuse existing db connection for better performance
        Tapsoob::Utils.load_schema(dump_path, db, table)
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
      stream = Tapsoob::DataStream.factory(db, stream_state)
      chunksize = stream_state[:chunksize] || default_chunksize
      estimated_chunks = [(record_count.to_f / chunksize).ceil, 1].max
      progress = (opts[:progress] ? ProgressBar.new(table_name.to_s, estimated_chunks) : nil)
      push_data_from_file(stream, progress)
    end

    def push_data
      log.info "Sending data"

      log.info "#{tables.size} tables, #{format_number(record_count)} records"

      if parallel?
        push_data_parallel
      else
        push_data_serial
      end
    end

    def push_data_serial
      max_visible_bars = 8
      multi_progress = opts[:progress] ? MultiProgressBar.new(max_visible_bars) : nil

      tables.each do |table_name, count|
        # Skip if data file doesn't exist or has no data
        data_file = File.join(dump_path, "data", "#{table_name}.json")
        next unless File.exist?(data_file) && count > 0

        # Check if this table should use intra-table parallelization
        table_workers = table_parallel_workers(table_name, count)

        if table_workers > 1
          info_msg = "Table #{table_name}: using #{table_workers} workers for #{format_number(count)} records"
          multi_progress.set_info(info_msg) if multi_progress
          push_data_from_file_parallel(table_name, count, table_workers, multi_progress)
        else
          # Show info message for single-worker table
          info_msg = "Loading #{table_name}: #{format_number(count)} records"
          multi_progress.set_info(info_msg) if multi_progress

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
          estimated_chunks = [(count.to_f / default_chunksize).ceil, 1].max
          progress = multi_progress ? multi_progress.create_bar(table_name.to_s, estimated_chunks) : nil
          push_data_from_file(stream, progress)
        end
      end

      multi_progress.stop if multi_progress
    end

    def push_data_parallel
      log.info "Using #{parallel_workers} parallel workers for table-level parallelization"

      # Reserve space for both table-level and intra-table workers
      max_visible_bars = 8
      multi_progress = opts[:progress] ? MultiProgressBar.new(max_visible_bars) : nil
      table_queue = Queue.new

      tables.each do |table_name, count|
        data_file = File.join(dump_path, "data", "#{table_name}.json")
        next unless File.exist?(data_file) && count > 0
        table_queue << [table_name, count]
      end

      workers = (1..parallel_workers).map do
        Thread.new do
          loop do
            break if table_queue.empty?

            table_name, count = table_queue.pop(true) rescue break

            # Check if this table should use intra-table parallelization
            table_workers = table_parallel_workers(table_name, count)

            if table_workers > 1
              # Large table - use intra-table parallelization
              info_msg = "Table #{table_name}: using #{table_workers} workers for #{format_number(count)} records"
              if multi_progress
                multi_progress.set_info(info_msg)
              else
                log.info info_msg
              end

              # Run intra-table parallelization, passing parent progress bar
              push_data_from_file_parallel(table_name, count, table_workers, multi_progress)
            else
              # Small table - use single-threaded processing
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

              estimated_chunks = [(count.to_f / default_chunksize).ceil, 1].max
              progress = multi_progress ? multi_progress.create_bar(table_name.to_s, estimated_chunks) : nil

              push_data_from_file(stream, progress)
            end
          end
        end
      end

      workers.each(&:join)
      multi_progress.stop if multi_progress
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

        # Update progress bar by 1 chunk
        progress.inc(1) if progress

        break if stream.complete?
      end

      progress.finish if progress
      add_completed_table(stream.table_name)
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

      # Try to load table order from metadata file (preserves dependency order)
      ordered_tables = load_table_order

      if ordered_tables
        # Use the saved dependency order
        table_names = ordered_tables
      else
        # Fallback: read from schema files (alphabetical order - may have issues with dependencies)
        table_names = Dir.glob(File.join(dump_path, "schemas", "*"))
          .map { |path| File.basename(path, ".rb") }
          .reject { |name| name == "table_order" }  # Exclude metadata file
          .sort
      end

      # Count rows for each table
      table_names.each do |table|
        if File.exist?(File.join(dump_path, "data", "#{table}.json"))
          # Read NDJSON format - each line is a separate JSON chunk
          total_rows = 0
          File.readlines(File.join(dump_path, "data", "#{table}.json")).each do |line|
            chunk = JSON.parse(line.strip)
            total_rows += chunk["data"].size if chunk["data"]
          end
          tables_with_counts[table] = total_rows
        else
          tables_with_counts[table] = 0
        end
      end

      apply_table_filter(tables_with_counts)
    end

    # Calculate line ranges for file partitioning
    def calculate_file_line_ranges(table_name, num_workers)
      file_path = File.join(dump_path, "data", "#{table_name}.json")
      return [] unless File.exist?(file_path)

      total_lines = File.foreach(file_path).count
      return [[0, total_lines - 1]] if total_lines == 0 || num_workers <= 1

      lines_per_worker = (total_lines.to_f / num_workers).ceil

      ranges = []
      (0...num_workers).each do |i|
        start_line = i * lines_per_worker
        end_line = [((i + 1) * lines_per_worker) - 1, total_lines - 1].min
        ranges << [start_line, end_line] if start_line < total_lines
      end

      ranges
    end

    # Parallel push for a single large table using file partitioning
    def push_data_from_file_parallel(table_name, count, num_workers, parent_progress = nil)
      # Calculate line ranges for each worker
      ranges = calculate_file_line_ranges(table_name, num_workers)
      return if ranges.empty?

      # Truncate table if purge is enabled
      db[table_name.to_sym].truncate if @opts[:purge]

      # Create single shared progress bar for the table
      estimated_chunks = [(count.to_f / default_chunksize).ceil, 1].max
      shared_progress = parent_progress ? parent_progress.create_bar(table_name.to_s, estimated_chunks) : nil

      # Mutex for coordinating database writes
      write_mutex = Mutex.new

      workers = (0...num_workers).map do |worker_id|
        Thread.new do
          start_line, end_line = ranges[worker_id]

          # Create worker-specific stream with line range
          stream = Tapsoob::DataStreamFilePartition.new(db, {
            :table_name => table_name,
            :chunksize => default_chunksize,
            :line_range => [start_line, end_line]
          }, {
            :"skip-duplicates" => opts[:"skip-duplicates"] || false,
            :"discard-identity" => opts[:"discard-identity"] || false,
            :purge => opts[:purge] || false,
            :debug => opts[:debug]
          })

          # Process chunks from file
          loop do
            break if stream.complete?

            begin
              encoded_data, row_size, elapsed_time = stream.fetch(:type => "file", :source => dump_path)

              if row_size.positive?
                data = {
                  :state => stream.to_hash,
                  :checksum => Tapsoob::Utils.checksum(encoded_data).to_s,
                  :encoded_data => encoded_data
                }

                # Thread-safe database write
                write_mutex.synchronize do
                  stream.fetch_data_to_database(data)
                end

                # Update shared progress bar
                shared_progress.inc(1) if shared_progress
              end

            rescue Tapsoob::CorruptedData => e
              log.info "Worker #{worker_id}: Corrupted Data Received #{e.message}, retrying..."
              next
            rescue StandardError => e
              log.error "Worker #{worker_id} error: #{e.message}"
              log.error e.backtrace.join("\n")
              raise
            end

            # Check completion after importing data
            break if stream.complete?
          end
        end
      end

      # Wait for all workers to complete
      workers.each(&:join)

      # Finish the shared progress bar
      shared_progress.finish if shared_progress
    end
  end
end
