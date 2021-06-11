require 'thor'
require 'fileutils'

require_relative '../config'
require_relative '../log'
require_relative '../operation'
require_relative '../version'

module Tapsoob
  module CLI
    class Root < Thor
      desc "pull DUMP_PATH DATABASE_URL", "Pull a dump from a database to a folder"
      option :"skip-schema", desc: "Don't transfer the schema just data", default: false, type: :boolean, aliases: "-s"
      option :"indexes-first", desc: "Transfer indexes first before data", default: false, type: :boolean, aliases: "-i"
      option :resume, desc: "Resume a Tapsoob Session from a stored file", type: :string, aliases: "-r"
      option :chunksize, desc: "Initial chunksize", default: 1000, type: :numeric, aliases: "-c"
      option :"disable-compression", desc: "Disable Compression", default: false, type: :boolean, aliases: "-g"
      option :filter, desc: "Regex Filter for tables", type: :string, aliases: "-f"
      option :tables, desc: "Shortcut to filter on a list of tables", type: :array, aliases: "-t"
      option :"exclude-tables", desc: "Shortcut to exclude a list of tables", type: :array, aliases: "-e"
      option :debug, desc: "Enable debug messages", default: false, type: :boolean, aliases: "-d"
      def pull(dump_path, database_url)
        opts = parse_opts(options)
        Tapsoob.log.level = Logger::DEBUG if opts[:debug]
        if opts[:resume_filename]
          clientresumexfer(:pull, dump_path, database_url, opts)
        else
          clientxfer(:pull, dump_path, database_url, opts)
        end
      end

      desc "push DUMP_PATH DATABASE_URL", "Push a previously tapsoob dump to a database"
      option :"skip-schema", desc: "Don't transfer the schema just data", default: false, type: :boolean, aliases: "-s"
      option :"indexes-first", desc: "Transfer indexes first before data", default: false, type: :boolean, aliases: "-i"
      option :resume, desc: "Resume a Tapsoob Session from a stored file", type: :string, aliases: "-r"
      option :chunksize, desc: "Initial chunksize", default: 1000, type: :numeric, aliases: "-c"
      option :"disable-compression", desc: "Disable Compression", default: false, type: :boolean, aliases: "-g"
      option :filter, desc: "Regex Filter for tables", type: :string, aliases: "-f"
      option :tables, desc: "Shortcut to filter on a list of tables", type: :array, aliases: "-t"
      option :"exclude-tables", desc: "Shortcut to exclude a list of tables", type: :array, aliases: "-e"
      option :purge, desc: "Purge data in tables prior to performing the import", default: false, type: :boolean, aliases: "-p"
      option :"discard-identity", desc: "Remove identity when pushing data (may result in creating duplicates)", default: false, type: :boolean
      option :debug, desc: "Enable debug messages", default: false, type: :boolean, aliases: "-d"
      def push(dump_path, database_url)
        opts = parse_opts(options)
        Tapsoob.log.level = Logger::DEBUG if opts[:debug]
        if opts[:resume_filename]
          clientresumexfer(:push, dump_path, database_url, opts)
        else
          clientxfer(:push, dump_path, database_url, opts)
        end
      end

      desc "version", "Show tapsoob version"
      def version
        puts Tapsoob::VERSION.dup
      end

      desc "schema SUBCOMMAND ...ARGS", "Direct access to Tapsoob::Schema class methods"
      subcommand "schema", Schema

      desc "data SUBCOMMAND ...ARGS", "Pull/Push data with internal Tapsoob classes"
      subcommand "data", DataStream

      private
        def parse_opts(options)
          # Default options
          opts = {
            skip_schema: options[:"skip-schema"],
            indexes_first: options[:"indexes_first"],
            disable_compression: options[:"disable-compression"],
            debug: options[:debug]
          }

          # Push only options
          opts[:purge] = options[:purge] if options.key?(:purge)
          opts[:"discard-identity"] = options[:"discard-identity"] if options.key?(:"discard-identity")

          # Resume
          if options[:resume]
            if File.exists?(options[:resume])
              opts[:resume_file] = options[:resume]
            else
              raise "Unable to find resume file."
            end
          end

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

        def clientxfer(method, dump_path, database_url, opts)
          Tapsoob::Config.verify_database_url(database_url)

          FileUtils.mkpath "#{dump_path}/schemas"
          FileUtils.mkpath "#{dump_path}/data"
          FileUtils.mkpath "#{dump_path}/indexes"

          Tapsoob::Operation.factory(method, database_url, dump_path, opts).run
        end

        def clientresumexfer(method, dump_path, database_url, opts)
          session = JSON.parse(File.read(opts.delete(:resume_filename)))
          session.symbolize_recursively!

          dump_path = dump_path || session.delete(:dump_path)

          require 'taps/operation'

          newsession = session.merge({
            :default_chunksize => opts[:default_chunksize],
            :disable_compression => opts[:disable_compression],
            :resume => true
          })

          Tapsoob::Operation.factory(method, database_url, dump_path, newsession).run
        end
    end
  end
end
