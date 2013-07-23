namespace :tapsoob do
  desc "Pulls a database to your filesystem"
  task :pull => :environment do
    Tapsoob::Operation.factory(:pull, database_uri, dump_path)
  end

  desc "Push a compatible dump on your filesystem to a database"
  task :push => :environment do
  end

  private
    def database_uri
      connection_config = ActiveRecord::Base.connection_config

      if (connection_config[:adapter] =~ /sqlite/i).nil?
        "#{connection_config[:adapter]}://#{connection_config[:username]}:#{connection_config[:password]}@#{connection_config[:host]}/#{connection_config[:database]}"
      else
        "#{connection_config[:adapter]}://#{connection_config[:adapter]}"
      end
    end

    def dump_path
      File.expand_path(Rails.root.join("db", Time.now.strftime("%Y%m%d%I%M%S%p"))).to_s
    end

    def latest_dump_path
    end
end