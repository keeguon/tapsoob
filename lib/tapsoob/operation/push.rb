# -*- encoding : utf-8 -*-
require 'tapsoob/operation/base'
require 'tapsoob/progress_event'

module Tapsoob
  module Operation
    class Push < Base
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
        Tapsoob::ProgressEvent.indexes_start(filtered_idxs.size)
        max_title_width = filtered_idxs.keys.map { |table| "#{table} indexes".length }.max || 14

        filtered_idxs.each do |table, indexes|
          progress = opts[:progress] ? Tapsoob::Progress::Bar.new("#{table} indexes", indexes.size, STDOUT, max_title_width) : nil
          indexes.each do |idx|
            Tapsoob::Utils.load_indexes(database_url, idx)
            progress.inc(1) if progress
          end
          progress.finish if progress
        end
        Tapsoob::ProgressEvent.indexes_complete(filtered_idxs.size)
      end

      def push_schema
        log.info "Sending schema"
        Tapsoob::ProgressEvent.schema_start(tables.size)

        progress = opts[:progress] ? Tapsoob::Progress::Bar.new('Schema', tables.size) : nil
        tables.each do |table, count|
          log.debug "Loading '#{table}' schema\n"
          # Reuse existing db connection for better performance
          Tapsoob::Utils.load_schema(dump_path, db, table)
          progress.inc(1) if progress
        end
        progress.finish if progress
        Tapsoob::ProgressEvent.schema_complete(tables.size)
      end

      def push_reset_sequences
        log.info "Resetting sequences"
        Tapsoob::ProgressEvent.sequences_start

        Tapsoob::Utils.schema_bin(:reset_db_sequences, database_url)

        Tapsoob::ProgressEvent.sequences_complete
      end

      def push_partial_data
        return if stream_state == {}

        table_name = stream_state[:table_name]
        record_count = tables[table_name.to_s]
        log.info "Resuming #{table_name}, #{format_number(record_count)} records"
        stream = Tapsoob::DataStream::Base.factory(db, stream_state)
        chunksize = stream_state[:chunksize] || default_chunksize
        estimated_chunks = [(record_count.to_f / chunksize).ceil, 1].max
        progress = (opts[:progress] ? Tapsoob::Progress::Bar.new(table_name.to_s, estimated_chunks) : nil)
        push_data_from_file(stream, progress)
      end

      def push_data
        log.info "Sending data"

        log.info "#{tables.size} tables, #{format_number(record_count)} records"
        Tapsoob::ProgressEvent.data_start(tables.size, record_count)

        if parallel?
          push_data_parallel
        else
          push_data_serial
        end

        Tapsoob::ProgressEvent.data_complete(tables.size, record_count)
      end

      def push_data_serial
        max_visible_bars = 8
        multi_progress = opts[:progress] ? Tapsoob::Progress::MultiBar.new(max_visible_bars) : nil

        tables.each do |table_name, count|
          # Skip if data file doesn't exist or has no data
          data_file = File.join(dump_path, "data", "#{table_name}.json")
          next unless File.exist?(data_file) && count > 0

          # Check if this table should use intra-table parallelization
          table_workers = table_parallel_workers(table_name, count)

          if table_workers > 1
            info_msg = "Table #{table_name}: using #{table_workers} workers for #{format_number(count)} records"
            multi_progress.set_info(info_msg) if multi_progress
            Tapsoob::ProgressEvent.table_start(table_name, count, workers: table_workers)
            push_data_from_file_parallel(table_name, count, table_workers, multi_progress)
          else
            # Show info message for single-worker table
            info_msg = "Loading #{table_name}: #{format_number(count)} records"
            multi_progress.set_info(info_msg) if multi_progress

            Tapsoob::ProgressEvent.table_start(table_name, count)
            db[table_name.to_sym].truncate if @opts[:purge]
            stream = Tapsoob::DataStream::Base.factory(db, {
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
            push_data_from_file(stream, progress, count)
          end
        end

        multi_progress.stop if multi_progress
      end

      def push_data_parallel
        log.info "Using #{parallel_workers} parallel workers for table-level parallelization"

        # Reserve space for both table-level and intra-table workers
        max_visible_bars = 8
        multi_progress = opts[:progress] ? Tapsoob::Progress::MultiBar.new(max_visible_bars) : nil
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

                Tapsoob::ProgressEvent.table_start(table_name, count, workers: table_workers)
                # Run intra-table parallelization, passing parent progress bar
                push_data_from_file_parallel(table_name, count, table_workers, multi_progress)
              else
                # Small table - use single-threaded processing
                Tapsoob::ProgressEvent.table_start(table_name, count)
                db[table_name.to_sym].truncate if @opts[:purge]
                stream = Tapsoob::DataStream::Base.factory(db, {
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

                push_data_from_file(stream, progress, count)
              end
            end
          end
        end

        workers.each(&:join)
        multi_progress.stop if multi_progress
      end

      def push_data_from_file(stream, progress, total_records = nil)
        records_processed = 0

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

              # Track records for progress events
              if row_size.positive?
                records_processed += row_size
                Tapsoob::ProgressEvent.table_progress(stream.table_name, records_processed, total_records) if total_records
              end
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

        # Emit final table complete event
        Tapsoob::ProgressEvent.table_complete(stream.table_name, records_processed)
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

        # Mutex for coordinating database writes and progress tracking
        write_mutex = Mutex.new
        records_processed = 0

        begin
          workers = (0...num_workers).map do |worker_id|
            Thread.new do
              start_line, end_line = ranges[worker_id]

              # Create worker-specific stream with line range
              stream = Tapsoob::DataStream::FilePartition.new(db, {
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

                    # Thread-safe database write and progress tracking
                    write_mutex.synchronize do
                      stream.fetch_data_to_database(data)

                      # Track records for progress events
                      records_processed += row_size
                      Tapsoob::ProgressEvent.table_progress(table_name, records_processed, count)
                    end

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

                break if stream.complete?
              end
            end
          end

          workers.each(&:join)
          shared_progress.finish if shared_progress
        ensure
          # Always emit table_complete event, even if there was an error
          Tapsoob::ProgressEvent.table_complete(table_name, records_processed)
        end
      end
    end
  end
end
