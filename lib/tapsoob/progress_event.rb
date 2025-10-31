# -*- encoding : utf-8 -*-
require 'json'

module Tapsoob
  module ProgressEvent
    @last_progress_time = {}
    @progress_throttle = 0.5  # Emit progress at most every 0.5 seconds per table
    @enabled = false  # Only emit when CLI progress bars are disabled

    def self.enabled=(value)
      @enabled = value
    end

    def self.enabled?
      @enabled
    end

    # Emit structured JSON progress events to STDERR for machine parsing
    # Only emits when enabled (typically when CLI progress bars are disabled)
    def self.emit(event_type, data = {})
      return unless @enabled
      event = {
        event: event_type,
        timestamp: Time.now.utc.iso8601
      }.merge(data)

      STDERR.puts "PROGRESS: #{JSON.generate(event)}"
      STDERR.flush
    end

    # Check if enough time has passed to emit a progress event for this table
    def self.should_emit_progress?(table_name)
      now = Time.now
      last_time = @last_progress_time[table_name]

      if last_time.nil? || (now - last_time) >= @progress_throttle
        @last_progress_time[table_name] = now
        true
      else
        false
      end
    end

    # Clear throttle state for a table (call when table completes)
    def self.clear_throttle(table_name)
      @last_progress_time.delete(table_name)
    end

    # Schema events
    def self.schema_start(table_count)
      emit('schema_start', tables: table_count)
    end

    def self.schema_complete(table_count)
      emit('schema_complete', tables: table_count)
    end

    # Data events
    def self.data_start(table_count, record_count)
      emit('data_start', tables: table_count, records: record_count)
    end

    def self.data_complete(table_count, record_count)
      emit('data_complete', tables: table_count, records: record_count)
    end

    # Table-level events
    def self.table_start(table_name, record_count, workers: 1)
      clear_throttle(table_name)  # Reset throttle for new table
      emit('table_start', table: table_name, records: record_count, workers: workers)
    end

    def self.table_progress(table_name, current, total)
      # Throttle progress events to avoid spam
      return unless should_emit_progress?(table_name)

      percentage = total > 0 ? ((current.to_f / total) * 100).round(1) : 0
      emit('table_progress', table: table_name, current: current, total: total, percentage: percentage)
    end

    def self.table_complete(table_name, record_count)
      clear_throttle(table_name)  # Clean up throttle state
      emit('table_complete', table: table_name, records: record_count)
    end

    # Index events
    def self.indexes_start(table_count)
      emit('indexes_start', tables: table_count)
    end

    def self.indexes_complete(table_count)
      emit('indexes_complete', tables: table_count)
    end

    # Sequence events
    def self.sequences_start
      emit('sequences_start')
    end

    def self.sequences_complete
      emit('sequences_complete')
    end

    # Error events
    def self.error(message, context = {})
      emit('error', { message: message }.merge(context))
    end
  end
end
