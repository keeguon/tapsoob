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
      option :filter, desc: "Regex Filter for tables", type: :string, aliases: "-f"
      option :tables, desc: "Shortcut to filter on a list of tables", type: :array, aliases: "-t"
      option :"exclude-tables", desc: "Shortcut to exclude a list of tables", type: :array, aliases: "-e"
      option :progress, desc: "Show progress", default: true, type: :boolean, aliases: "-p"
      option :debug, desc: "Enable debug messages", default: false, type: :boolean, aliases: "-d"
      def pull(database_url, dump_path = nil)
        op = Tapsoob::Operation.factory(:pull, database_url, dump_path, parse_opts(options))
        op.pull_data
      end

      desc "push DATABASE_URL [DUMP_PATH]", "Push data to a database."
      option :chunksize, desc: "Initial chunksize", default: 1000, type: :numeric, aliases: "-c"
      option :filter, desc: "Regex Filter for tables", type: :string, aliases: "-f"
      option :tables, desc: "Shortcut to filter on a list of tables", type: :array, aliases: "-t"
      option :"exclude-tables", desc: "Shortcut to exclude a list of tables", type: :array, aliases: "-e"
      option :progress, desc: "Show progress", default: true, type: :boolean, aliases: "-p"
      option :purge, desc: "Purge data in tables prior to performing the import", default: false, type: :boolean, aliases: "-p"
      option :"skip-duplicates", desc: "Remove duplicates when loading data", default: false, type: :boolean
      option :"discard-identity", desc: "Remove identity when pushing data (may result in creating duplicates)", default: false, type: :boolean
      option :debug, desc: "Enable debug messages", default: false, type: :boolean, aliases: "-d"
      def push(database_url, dump_path = nil)
        # instantiate stuff
        data = []
        opts = parse_opts(options)

        # read data from dump_path or from STDIN
        if dump_path && Dir.exists?(dump_path)
          files = Dir[Pathname.new(dump_path).join("*.json")]
          files.each { |file| data << JSON.parse(File.read(file), symbolize_names: true) }
        else
          STDIN.each_line { |line| data << JSON.parse(line, symbolize_names: true) }
        end

        # import data
        data.each do |table|
          stream = Tapsoob::DataStream.factory(db(database_url, opts), {
            table_name: table[:table_name],
            chunksize: opts[:default_chunksize]
          }, { :"discard-identity" => opts[:"discard-identity"] || false, :purge => opts[:purge] || false, :debug => opts[:debug] })

          begin
            stream.import_rows(table)
          rescue Exception => e
            stream.log.debug e.message
          end
        end
      end

      private
        def parse_opts(options)
          # Default options
          opts = {
            progress: options[:progress],
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

          # Regex filter
          opts[:table_filter] = options[:filter] if options[:filter]

          # Table filter
          if options[:tables]
            r_tables = options[:tables].collect { |t| "^#{t}" }.join("|")
            opts[:table_filter] = "#{r_tables}"
          end

          # Exclude tables
          opts[:exclude_tables] = options[:"exclude-tables"] if options[:"exclude-tables"]

          opts
        end

        def db(database_url, opts = {})
          @db ||= Sequel.connect(database_url)
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
