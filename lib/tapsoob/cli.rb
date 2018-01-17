# -*- encoding : utf-8 -*-
require 'fileutils'
require 'optparse'
require 'tempfile'
require 'tapsoob/config'
require 'tapsoob/log'

Tapsoob::Config.tapsoob_database_url = ENV['TAPSOOB_DATABASE_URL'] || begin
  # this is dirty but it solves a weird problem where the tempfile disappears mid-process
  #require ((RUBY_PLATFORM =~ /java/).nil? ? 'sqlite3' : 'jdbc-sqlite3')
  $__taps_database = Tempfile.new('tapsoob.db')
  $__taps_database.open()
  "sqlite://#{$__taps_database.path}"
end

module Tapsoob
  class Cli
    attr_accessor :argv

    def initialize(argv)
      @argv = argv
    end

    def run
      method = (argv.shift || 'help').to_sym
      if [:pull, :push, :version].include? method
        send(method)
      else
        help
      end
    end

    def pull
      opts = clientoptparse(:pull)
      Tapsoob.log.level = Logger::DEBUG if opts[:debug]
      if opts[:resume_filename]
        clientresumexfer(:pull, opts)
      else
        clientxfer(:pull, opts)
      end
    end

    def push
      opts = clientoptparse(:push)
      Tapsoob.log.level = Logger::DEBUG if opts[:debug]
      if opts[:resume_filename]
        clientresumexfer(:push, opts)
      else
        clientxfer(:push, opts)
      end
    end

    def version
      puts Tapsoob.version
    end

    def help
      puts <<EOHELP
Options
=======
pull      Pull a database and export it into a directory
push      Push a database from a directory
version   Tapsoob version

Add '-h' to any command to see their usage
EOHELP
    end

    def clientoptparse(cmd)
      opts={:default_chunksize => 1000, :database_url => nil, :dump_path => nil, :debug => false, :resume_filename => nil, :disable_compression => false, :indexes_first => false}
      OptionParser.new do |o|
        o.banner = "Usage: #{File.basename($0)} #{cmd} [OPTIONS] <dump_path> <database_url>"

        case cmd
        when :pull
          o.define_head "Pull a database and export it into a directory"
        when :push
          o.define_head "Push a database from a directory"
        end

        o.on("-s", "--skip-schema", "Don't transfer the schema just data") { |v| opts[:skip_schema] = true }
        o.on("-i", "--indexes-first", "Transfer indexes first before data") { |v| opts[:indexes_first] = true }
        o.on("-r", "--resume=file", "Resume a Tapsoob Session from a stored file") { |v| opts[:resume_filename] = v }
        o.on("-c", "--chunksize=N", "Initial Chunksize") { |v| opts[:default_chunksize] = (v.to_i < 10 ? 10 : v.to_i) }
        o.on("-g", "--disable-compression", "Disable Compression") { |v| opts[:disable_compression] = true }
        o.on("-f", "--filter=regex", "Regex Filter for tables") { |v| opts[:table_filter] = v }
        o.on("-t", "--tables=A,B,C", Array, "Shortcut to filter on a list of tables") do |v|
          r_tables = v.collect { |t| "^#{t}" }.join("|")
          opts[:table_filter] = "#{r_tables}"
        end
        o.on("-e", "--exclude-tables=A,B,C", Array, "Shortcut to exclude a list of tables") { |v| opts[:exclude_tables] = v }
        o.on("-d", "--debug", "Enable Debug Messages") { |v| opts[:debug] = true }

        opts[:dump_path] = argv.shift
        opts[:database_url] = argv.shift

        if opts[:database_url].nil?
          $stderr.puts "Missing Database URL"
          puts o
          exit 1
        end
        if opts[:dump_path].nil?
          $stderr.puts "Missing Tapsoob Dump Path"
          puts o
          exist 1
        end
      end

      opts
    end

    def clientxfer(method, opts)
      database_url = opts.delete(:database_url)
      dump_path = opts.delete(:dump_path)

      Tapsoob::Config.verify_database_url(database_url)

      FileUtils.mkpath "#{dump_path}/schemas"
      FileUtils.mkpath "#{dump_path}/data"
      FileUtils.mkpath "#{dump_path}/indexes"

      require 'tapsoob/operation'

      Tapsoob::Operation.factory(method, database_url, dump_path, opts).run
    end

    def clientresumexfer(method, opts)
      session = JSON.parse(File.read(opts.delete(:resume_filename)))
      session.symbolize_recursively!

      database_url = opts.delete(:database_url)
      dump_path = opts.delete(:dump_path) || session.delete(:dump_path)

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
