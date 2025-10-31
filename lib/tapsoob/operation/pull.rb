# -*- encoding : utf-8 -*-
require 'fileutils'
require 'tapsoob/operation/base'
require 'tapsoob/progress_event'

module Tapsoob
  module Operation
    class Pull < Base
      def file_prefix
        "pull"
      end

      def to_hash
        super.merge(:remote_tables_info => remote_tables_info)
      end

      def run
        catch_errors do
          unless resuming?
            initialize_dump_directory if dump_path
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

      def initialize_dump_directory
        %w[data schemas indexes].each do |subdir|
          dir_path = File.join(dump_path, subdir)
          FileUtils.rm_rf(dir_path)
          FileUtils.mkdir_p(dir_path)
        end

        FileUtils.rm_f(File.join(dump_path, "table_order.txt"))
      end

      def pull_schema
        log.info "Receiving schema"
        Tapsoob::ProgressEvent.schema_start(tables.size)

        progress = opts[:progress] ? Tapsoob::Progress::Bar.new('Schema', tables.size) : nil
        tables.each do |table_name, count|
          # Reuse existing db connection for better performance
          schema_data = Tapsoob::Schema.dump_table(db, table_name, @opts.slice(:indexes, :same_db))
          log.debug "Table: #{table_name}\n#{schema_data}\n"
          output = Tapsoob::Utils.export_schema(dump_path, table_name, schema_data)
          puts output if dump_path.nil? && output
          progress.inc(1) if progress
        end
        progress.finish if progress
        Tapsoob::ProgressEvent.schema_complete(tables.size)

        # Save table order for dependency-aware schema loading during push
        save_table_order(tables.keys) if dump_path
      end

      def pull_data
        log.info "Receiving data"

        log.info "#{tables.size} tables, #{format_number(record_count)} records"
        Tapsoob::ProgressEvent.data_start(tables.size, record_count)

        if parallel?
          pull_data_parallel
        else
          pull_data_serial
        end

        Tapsoob::ProgressEvent.data_complete(tables.size, record_count)
      end

      def pull_data_serial
        tables.each do |table_name, count|
          # Auto-detect if we should use intra-table parallelization
          table_workers = table_parallel_workers(table_name, count)

          if table_workers > 1
            log.info "Table #{table_name}: using #{table_workers} workers for #{format_number(count)} records"
            Tapsoob::ProgressEvent.table_start(table_name, count, workers: table_workers)
            pull_data_from_table_parallel(table_name, count, table_workers)
          else
            Tapsoob::ProgressEvent.table_start(table_name, count)
            stream = Tapsoob::DataStream::Base.factory(db, {
              :chunksize  => default_chunksize,
              :table_name => table_name
            }, { :debug => opts[:debug] })
            estimated_chunks = [(count.to_f / default_chunksize).ceil, 1].max
            progress = (opts[:progress] ? Tapsoob::Progress::Bar.new(table_name.to_s, estimated_chunks) : nil)
            pull_data_from_table(stream, progress, count)
          end
        end
      end

      def pull_data_parallel
        log.info "Using #{parallel_workers} parallel workers for table-level parallelization"

        # Reserve space for both table-level and intra-table workers
        # With 4 table workers and potentially 8 intra-table workers per table,
        # we could have many concurrent progress bars. Show up to 8 at once.
        max_visible_bars = 8
        multi_progress = opts[:progress] ? Tapsoob::Progress::MultiBar.new(max_visible_bars) : nil
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

                Tapsoob::ProgressEvent.table_start(table_name, count, workers: table_workers)
                # Run intra-table parallelization, passing parent progress bar
                pull_data_from_table_parallel(table_name, count, table_workers, multi_progress)
              else
                # Small table - use single-threaded processing
                Tapsoob::ProgressEvent.table_start(table_name, count)
                stream = Tapsoob::DataStream::Base.factory(db, {
                  :chunksize  => default_chunksize,
                  :table_name => table_name
                }, { :debug => opts[:debug] })

                estimated_chunks = [(count.to_f / default_chunksize).ceil, 1].max
                progress = multi_progress ? multi_progress.create_bar(table_name.to_s, estimated_chunks) : nil

                pull_data_from_table(stream, progress, count)
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

        stream = Tapsoob::DataStream::Base.factory(db, stream_state)
        chunksize = stream_state[:chunksize] || default_chunksize
        estimated_chunks = [(record_count.to_f / chunksize).ceil, 1].max
        progress = (opts[:progress] ? Tapsoob::Progress::Bar.new(table_name.to_s, estimated_chunks) : nil)
        pull_data_from_table(stream, progress)
      end

      def pull_data_from_table(stream, progress, total_records = nil)
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

                # Track records processed for progress events
                if rows[:data]
                  records_processed += rows[:data].size
                  Tapsoob::ProgressEvent.table_progress(stream.table_name, records_processed, total_records) if total_records
                end

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

        # Emit final table complete event
        Tapsoob::ProgressEvent.table_complete(stream.table_name, records_processed)
      end

      def pull_data_from_table_parallel(table_name, row_count, num_workers, parent_progress = nil)
        # Mutex for coordinating file writes and progress tracking
        write_mutex = Mutex.new
        records_processed = 0

        begin
          # Determine partitioning strategy
          use_pk_partitioning = can_use_pk_partitioning?(table_name)

          if use_pk_partitioning
            # PK-based partitioning for efficient range queries
            ranges = Tapsoob::DataStream::Keyed.calculate_pk_ranges(db, table_name, num_workers)
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
                stream = Tapsoob::DataStream::KeyedPartition.new(db, {
                  :table_name => table_name,
                  :chunksize => default_chunksize,
                  :partition_range => [min_pk, max_pk]
                }, { :debug => opts[:debug] })
              else
                stream = Tapsoob::DataStream::Interleaved.new(db, {
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

                      # Thread-safe file write and progress tracking
                      write_mutex.synchronize do
                        if dump_path.nil?
                          puts JSON.generate(rows)
                        else
                          Tapsoob::Utils.export_rows(dump_path, stream.table_name, rows)
                        end

                        # Track records for progress events
                        if rows[:data]
                          records_processed += rows[:data].size
                          Tapsoob::ProgressEvent.table_progress(table_name, records_processed, row_count)
                        end
                      end

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

          workers.each(&:join)
          shared_progress.finish if shared_progress

          add_completed_table(table_name)
        ensure
          # Always emit table_complete event, even if there was an error
          Tapsoob::ProgressEvent.table_complete(table_name, records_processed)
        end
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
          Tapsoob::DataStream::Keyed.new(db, state)
        else
          Tapsoob::DataStream::Base.new(db, state)
        end
      end

      def pull_indexes
        log.info "Receiving indexes"

        raw_idxs = Tapsoob::Schema.indexes_individual(database_url)
        idxs     = (raw_idxs && raw_idxs.length >= 2 ? JSON.parse(raw_idxs) : {})

        # Calculate max title width for consistent alignment
        filtered_idxs = apply_table_filter(idxs).select { |table, indexes| indexes.size > 0 }
        Tapsoob::ProgressEvent.indexes_start(filtered_idxs.size)
        max_title_width = filtered_idxs.keys.map { |table| "#{table} indexes".length }.max || 14

        filtered_idxs.each do |table, indexes|
          progress = opts[:progress] ? Tapsoob::Progress::Bar.new("#{table} indexes", indexes.size, STDOUT, max_title_width) : nil
          indexes.each do |idx|
            output = Tapsoob::Utils.export_indexes(dump_path, table, idx)
            puts output if dump_path.nil? && output
            progress.inc(1) if progress
          end
          progress.finish if progress
        end
        Tapsoob::ProgressEvent.indexes_complete(filtered_idxs.size)
      end

      def pull_reset_sequences
        log.info "Resetting sequences"
        Tapsoob::ProgressEvent.sequences_start

        output = Tapsoob::Utils.schema_bin(:reset_db_sequences, database_url)
        puts output if dump_path.nil? && output

        Tapsoob::ProgressEvent.sequences_complete
      end
    end
  end
end
