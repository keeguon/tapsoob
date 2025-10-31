require 'json'
require 'pathname'
require 'thor'
require 'sequel'

require_relative '../data_stream.rb'
require_relative '../log'
require_relative '../operation'

module Tapsoob
  module CLI
    class DataStream < Thor
      desc "pull DATABASE_URL [DUMP_PATH]", "Pull data from a database."
      option :chunksize, desc: "Initial chunksize", default: 1000, type: :numeric, aliases: "-c"
      option :tables, desc: "Shortcut to filter on a list of tables", type: :array, aliases: "-t"
      option :"exclude-tables", desc: "Shortcut to exclude a list of tables", type: :array, aliases: "-e"
      option :parallel, desc: "Number of parallel workers for table processing (default: 1)", default: 1, type: :numeric, aliases: "-j"
      option :progress, desc: "Show progress", default: true, type: :boolean, aliases: "-p"
      option :debug, desc: "Enable debug messages", default: false, type: :boolean, aliases: "-d"
      def pull(database_url, dump_path = nil)
        opts = parse_opts(options)

        # Force serial mode when outputting to STDOUT (for piping)
        # Parallel mode would interleave output and corrupt the JSON stream
        if dump_path.nil? && opts[:parallel] && opts[:parallel] > 1
          STDERR.puts "Warning: Parallel mode disabled when outputting to STDOUT (for piping)"
          opts[:parallel] = 1
        end

        op = Tapsoob::Operation::Base.factory(:pull, database_url, dump_path, opts)
        op.pull_data
      end

      desc "push DATABASE_URL [DUMP_PATH]", "Push data to a database."
      option :chunksize, desc: "Initial chunksize", default: 1000, type: :numeric, aliases: "-c"
      option :tables, desc: "Shortcut to filter on a list of tables", type: :array, aliases: "-t"
      option :"exclude-tables", desc: "Shortcut to exclude a list of tables", type: :array, aliases: "-e"
      option :parallel, desc: "Number of parallel workers for table processing (default: 1)", default: 1, type: :numeric, aliases: "-j"
      option :progress, desc: "Show progress", default: true, type: :boolean, aliases: "-p"
      option :purge, desc: "Purge data in tables prior to performing the import", default: false, type: :boolean
      option :"skip-duplicates", desc: "Remove duplicates when loading data", default: false, type: :boolean
      option :"discard-identity", desc: "Remove identity when pushing data (may result in creating duplicates)", default: false, type: :boolean
      option :debug, desc: "Enable debug messages", default: false, type: :boolean, aliases: "-d"
      def push(database_url, dump_path = nil)
        opts = parse_opts(options)

        # If dump_path is provided, use the Operation class for proper parallel support
        if dump_path && Dir.exist?(dump_path)
          op = Tapsoob::Operation::Base.factory(:push, database_url, dump_path, opts)
          op.push_data
        else
          # STDIN mode: read and import data directly (no parallel support for STDIN)
          if opts[:parallel] && opts[:parallel] > 1
            STDERR.puts "Warning: Parallel mode not supported when reading from STDIN"
          end

          data = []
          STDIN.each_line { |line| data << JSON.parse(line, symbolize_names: true) }

          # import data
          data.each do |table|
            table_name = table[:table_name]

            # Truncate table if purge option is enabled
            if opts[:purge]
              db(database_url, opts)[table_name.to_sym].truncate
            end

            stream = Tapsoob::DataStream::Base.factory(db(database_url, opts), {
              table_name: table_name,
              chunksize: opts[:default_chunksize]
            }, { :"discard-identity" => opts[:"discard-identity"] || false, :purge => opts[:purge] || false, :debug => opts[:debug] })

            begin
              stream.import_rows(table)
            rescue Exception => e
              stream.log.debug e.message
              STDERR.puts "Error loading data in #{table_name} : #{e.message}"
            end
          end
        end
      end

      private
        def parse_opts(options)
          # Default options
          opts = {
            progress: options[:progress],
            tables: options[:tables],
            parallel: options[:parallel],
            debug: options[:debug]
          }

          # Push only options
          opts[:purge] = options[:purge] if options.key?(:purge)
          opts[:"skip-duplicates"] = options[:"skip-duplicates"] if options.key?(:"skip-duplicates")
          opts[:"discard-identity"] = options[:"discard-identity"] if options.key?(:"discard-identity")

          # Default chunksize
          if options[:chunksize]
            opts[:default_chunksize] = (options[:chunksize] < 10 ? 10 : options[:chunksize])
          end

          # Exclude tables
          opts[:exclude_tables] = options[:"exclude-tables"] if options[:"exclude-tables"]

          opts
        end

        def db(database_url, opts = {})
          # Support connection pooling for parallel operations
          parallel_workers = opts[:parallel] || 1
          @db ||= Sequel.connect(database_url, max_connections: parallel_workers * 2)
          @db.loggers << Tapsoob.log if opts[:debug]

          # Set parameters
          if @db.uri =~ /oracle/i
            @db << "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"
            @db << "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS:FF6'"
          end

          @db
        end
    end
  end
end
