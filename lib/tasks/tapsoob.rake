namespace :tapsoob do
  desc "Pulls a database to your filesystem"
  task :pull => :environment do
    # Default options
    opts={:default_chunksize => 1000, :debug => false, :resume_filename => nil, :disable_compression => false, :schema => true, :data => true, :indexes_first => false}

    # Get the dump_path
    dump_path = File.expand_path(Rails.root.join("db", Time.now.strftime("%Y%m%d%I%M%S%p"))).to_s

    # Create paths
    FileUtils.mkpath "#{dump_path}/schemas"
    FileUtils.mkpath "#{dump_path}/data"
    FileUtils.mkpath "#{dump_path}/indexes"

    # Run operation
    Tapsoob::Operation.factory(:pull, database_uri, dump_path, opts).run

    # Invoke cleanup task
    Rake::Task["tapsoob:clean"].reenable
    Rake::Task["tapsoob:clean"].invoke
  end

  desc "Push a compatible dump on your filesystem to a database"
  task :push, [:timestamp] => :environment do |t, args|
    # Default options
    opts={:default_chunksize => 1000, :debug => false, :resume_filename => nil, :disable_compression => false, :schema => true, :data => true, :indexes_first => false}

    # Get the dumps
    dumps = Dir[Rails.root.join("db", "*/")].select { |e| e =~ /([0-9]{14})([A-Z]{2})/ }.sort

    # In case a timestamp argument try to use it instead of using the last dump
    dump_path = dumps.last
    unless args[:timestamp].nil?
      timestamps = dumps.map { |dump| File.basename(dump) }

      # Check that the dump_path exists
      raise Exception.new "Invalid or non existent timestamp: '#{args[:timestamp]}'" unless timestamps.include?(args[:timestamp])

      # Select dump_path
      dump_path = Rails.root.join("db", args[:timestamp])
    end

    # Run operation
    Tapsoob::Operation.factory(:push, database_uri, dump_path, opts).run
  end

  desc "Cleanup old dumps"
  task :clean, [:keep] => :environment do |t, args|
    # Number of dumps to keep
    keep = ((args[:keep] =~ /\A[0-9]+\z/).nil? ? 5 : args[:keep].to_i)

    # Get all the dump folders
    dumps = Dir[Rails.root.join("db", "*/")].select { |e| e =~ /([0-9]{14})([A-Z]{2})/ }.sort

    # Delete old dumps only if there more than we want to keep
    if dumps.count > keep
      old_dumps = dumps - dumps.reverse[0..(keep - 1)]
      old_dumps.each do |dir|
        if Dir.exists?(dir)
          puts "Deleting old dump directory ('#{dir}')"
          FileUtils.remove_entry_secure(dir)
        end
      end
    end
  end

  private
    def database_uri
      uri               = ""
      connection_config = YAML::load(ERB.new(Rails.root.join("config", "database.yml").read).result)[Rails.env]

      case connection_config['adapter']
      when "mysql", "mysql2"
        uri = "#{connection_config['adapter']}://#{connection_config['host']}/#{connection_config['database']}?user=#{connection_config['username']}&password=#{connection_config['password']}"
      when "postgresql", "postgres", "pg"
        uri = "://#{connection_config['host']}/#{connection_config['database']}?user=#{connection_config['username']}&password=#{connection_config['password']}"
        uri = ((RUBY_PLATFORM =~ /java/).nil? ? "postgres" : "postgresql") + uri
      when "oracle_enhanced"
        if (RUBY_PLATFORM =~ /java/).nil?
          uri = "oracle://#{connection_config['host']}/#{connection_config['database']}?user=#{connection_config['username']}&password=#{connection_config['password']}"
        else
          uri = "oracle:thin:#{connection_config['username']}/#{connection_config['password']}@#{connection_config['host']}:1521:#{connection_config['database']}"
        end
      when "sqlite3", "sqlite"
        uri = "sqlite://#{connection_config['database']}"
      else
        raise Exception, "Unsupported database adapter."
        #uri = "#{connection_config['adapter']}://#{connection_config['host']}/#{connection_config['database']}?user=#{connection_config['username']}&password=#{connection_config['password']}"
      end

      uri = "jdbc:#{uri}" unless (RUBY_PLATFORM =~ /java/).nil?

      uri
    end
end
