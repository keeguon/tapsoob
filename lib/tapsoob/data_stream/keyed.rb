# -*- encoding : utf-8 -*-
require 'tapsoob/data_stream/base'

module Tapsoob
  module DataStream
    class Keyed < Base
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
          log.debug "DataStream::Keyed#load_buffer SQL -> #{ds.sql}"
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

        log.debug "DataStream::Keyed#verify_stream -> state: #{state.inspect}"
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
  end
end
