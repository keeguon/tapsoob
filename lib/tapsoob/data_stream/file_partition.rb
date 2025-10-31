# -*- encoding : utf-8 -*-
require 'tapsoob/data_stream/base'

module Tapsoob
  module DataStream
    # DataStream variant for file-based parallelized loading
    # Each worker reads a different portion of the NDJSON file
    class FilePartition < Base
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
          log.debug "DataStream::FilePartition#fetch_file: current_line=#{state[:current_line]} end_line=#{end_line} lines_to_read=#{lines_to_read} chunksize=#{state[:chunksize]} table=#{table_name}"

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

        log.debug "DataStream::FilePartition#fetch_file: read #{data_batch.size} rows in #{state[:current_line] - start_line} lines table=#{table_name}"

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
        log.debug "DataStream::FilePartition#complete? current_line=#{state[:current_line]} end_line=#{end_line} result=#{result} table=#{table_name}"
        result
      end
    end
  end
end
