# -*- encoding : utf-8 -*-
require 'sequel'
require 'thread'
require 'etc'

require 'tapsoob/data_stream'
require 'tapsoob/log'
require 'tapsoob/progress'
require 'tapsoob/progress_event'
require 'tapsoob/schema'

module Tapsoob
  module Operation
    class Base
      attr_reader :database_url, :dump_path, :opts

      def initialize(database_url, dump_path = nil, opts={})
        @database_url = database_url
        @dump_path    = dump_path
        @opts         = opts
        @exiting      = false

        # Enable JSON progress events only when:
        # 1. CLI progress bars are disabled (--progress=false), AND
        # 2. Not piping (dump_path is provided)
        # This prevents STDERR noise when piping and when using visual progress bars
        Tapsoob::ProgressEvent.enabled = !opts[:progress] && !dump_path.nil?
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
        # return 1 if self.is_a?(Tapsoob::Operation::Push)

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
          when :pull   then Tapsoob::Operation::Pull
          when :push   then Tapsoob::Operation::Push
          when :resume then eval(opts[:klass])
          else raise "Unknown Operation Type -> #{type}"
        end

        klass.new(database_url, dump_path, opts)
      end
    end
  end
end
