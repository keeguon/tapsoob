require 'sequel'

require 'tapsoob/progress_bar'
require 'tapsoob/schema'
require 'tapsoob/data_stream'

module Tapsoob
  class Operation
    attr_reader :database_url, :opts

    def initialize(database_url, opts={})
      @database_url = database_url
      @opts         = opts
      @exiting      = false
    end

    def file_prefix
      "op"
    end

    def skip_schema?
      !!opts[:skip_schema]
    end

    def indexes_first?
      !!opts[:indexes_first]
    end

    def table_filter
      opts[:table_filter]
    end

    def exclude_tables
      opts[:exclude_tables] || []
    end

    def apply_table_filter(tables)
      return tables unless table_filter || exclude_tables

      re = table_filter ? Regexp.new(table_filter) : nil
      if tables.kind_of?(Hash)
        ntables = {}
        tables.each do |t, d|
          if !exclude_tables.include?(t.to_s) && (!re || !re.match(t.to_s).nil?)
            ntables[t] = d
          end
        end
        ntables
      else
        tables.reject { |t| exclude_tables.include?(t.to_s) || (re && re.match(t.to_s).nil?) }
      end
    end

    def log
      Tapsoob.log
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

    def self.factory(type, database_url, opts)
      type = :resume if opts[:resume]
      klass = case type
        when :pull   then Tapsoob::Pull
        when :push   then Tapsoob::Push
        when :resume then eval(opts[:klass])
        else raise "Unknown Operation Type -> #{type}"
      end

      klass.new(database_url, opts)
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
        pull_data
        pull_indexes if !indexes_first? && !skip_schema?
        pull_reset_sequences
      end
    end

    def pull_schema
      puts "Receiving schema"

      progress = ProgressBar.new('Schema', tables.size)
      tables.each do |table_name, count|
        schema_data = Tapsoob::Schema.dump_table(database_url, table_name)
        log.debug "Table: #{table_name}\n#{schema_data}\n"
        log.debug schema_data
        progress.inc(1)
      end
      progress.finish
    end

    def pull_data
      puts "Receiving data"

      puts "#{tables.size} tables, #{format_number(record_count)} records"

      tables.each do |table_name, count|
        progress = ProgressBar.new(table_name.to_s, count)
        stream   = Taps::DataStream.factory(db, {
          :chunksize  => default_chunksize,
          :table_name => table_name
        })
        pull_data_from_table(stream, progress)
      end
    end

    def pull_data_from_table(stream, progress)
      loop do
        begin
          exit 0 if exiting?

          size = stream.fetch
          break if stream.complete?
          progress.inc(size) unless exiting?
          stream.error = false
          self.stream_state = stream.to_hash
        rescue Tapsoob::CorruptedDate => e
          puts "Corrupted Data Received #{e.message}, retrying..."
          stream.error = true
          next
        end
      end

      progress.finish
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

      if Taps::Utils.single_integer_primary_key(db, state[:table_name].to_sym)
        DataStreamKeyed.new(db, state)
      else
        DataStream.new(db, state)
      end
    end
  end

  class DataStreamKeyed < DataStream
    attr_accessor :buffer
  end
end
