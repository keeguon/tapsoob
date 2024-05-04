require 'thor'
require 'fileutils'
require 'yaml'

require_relative '../config'
require_relative '../log'
require_relative '../operation'
require_relative '../version'

module Tapsoob
  module CLI
    class Root < Thor
      desc "pull DUMP_PATH DATABASE_URL", "Pull a dump from a database to a folder"
      option :config, desc: "Define all options in a config file", type: :string, aliases: '-C'
      option :data, desc: "Pull the data to the database", default: true, type: :boolean, aliases: '-d'
      option :schema, desc: "Pull the schema to the database", default: true, type: :boolean, aliases: "-s"
      option :"indexes-first", desc: "Transfer indexes first before data", default: false, type: :boolean, aliases: "-i"
      option :resume, desc: "Resume a Tapsoob Session from a stored file", type: :string, aliases: "-r"
      option :chunksize, desc: "Initial chunksize", default: 1000, type: :numeric, aliases: "-c"
      option :"disable-compression", desc: "Disable Compression", default: false, type: :boolean, aliases: "-g"
      option :tables, desc: "Shortcut to filter on a list of tables", type: :array, aliases: "-t"
      option :"exclude-tables", desc: "Shortcut to exclude a list of tables", type: :array, aliases: "-e"
      option :"indexes", type: :boolean, default: false
      option :"same-db", type: :boolean, default: false
      option :progress, desc: "Show progress", default: true, type: :boolean
      option :debug, desc: "Enable debug messages", default: false, type: :boolean
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
      option :config, desc: "Define all options in a config file", type: :string, aliases: '-C'
      option :data, desc: "Push the data to the database", default: true, type: :boolean, aliases: '-d'
      option :schema, desc: "Push the schema to the database", default: true, type: :boolean, aliases: "-s"
      option :"indexes-first", desc: "Transfer indexes first before data", default: false, type: :boolean, aliases: "-i"
      option :resume, desc: "Resume a Tapsoob Session from a stored file", type: :string, aliases: "-r"
      option :chunksize, desc: "Initial chunksize", default: 1000, type: :numeric, aliases: "-c"
      option :"disable-compression", desc: "Disable Compression", default: false, type: :boolean, aliases: "-g"
      option :tables, desc: "Shortcut to filter on a list of tables", type: :array, aliases: "-t"
      option :"exclude-tables", desc: "Shortcut to exclude a list of tables", type: :array, aliases: "-e"
      option :purge, desc: "Purge data in tables prior to performing the import", default: false, type: :boolean, aliases: "-p"
      option :"skip-duplicates", desc: "Remove duplicates when loading data", default: false, type: :boolean
      option :"discard-identity", desc: "Remove identity when pushing data (may result in creating duplicates)", default: false, type: :boolean
      option :progress, desc: "Show progress", default: true, type: :boolean
      option :debug, desc: "Enable debug messages", default: false, type: :boolean
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
          # Load config file if it exist
          opts = ((options[:config] && File.exist?(options[:config])) ? YAML.load_file(options[:config]) : {})

          # Default options
          opts = opts.merge({
            data: options[:data],
            schema: options[:schema],
            indexes_first: options[:"indexes_first"],
            disable_compression: options[:"disable-compression"],
            tables: options[:tables],
            progress: options[:progress],
            debug: options[:debug]
          })

          # Pull only options
          opts[:indexes] = options[:"indexes"] if options.key?(:"indexes")
          opts[:same_db] = options[:"same-db"] if options.key?(:"same-db")

          # Push only options
          opts[:purge] = options[:purge] if options.key?(:purge)
          opts[:"skip-duplicates"] = options[:"skip-duplicates"] if options.key?(:"skip-duplicates")
          opts[:"discard-identity"] = options[:"discard-identity"] if options.key?(:"discard-identity")

          # Resume
          if options[:resume]
            if File.exist?(options[:resume])
              opts[:resume_file] = options[:resume]
            else
              raise "Unable to find resume file."
            end
          end

          # Default chunksize
          if options[:chunksize]
            opts[:default_chunksize] = (options[:chunksize] < 10 ? 10 : options[:chunksize])
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
