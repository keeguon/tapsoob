# -*- encoding : utf-8 -*-
require 'tapsoob/data_stream/base'

module Tapsoob
  module DataStream
    # DataStream variant for interleaved chunk-based partitioning (for tables without integer PK)
    class Interleaved < Base
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
        log.debug "DataStream::Interleaved#fetch_rows SQL -> #{ds.sql} (worker #{worker_id}/#{num_workers}, chunk #{chunk_number})"

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

        log.debug "DataStream::Interleaved#fetch state -> #{state.inspect}"

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
  end
end
