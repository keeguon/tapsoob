require 'thor'
require 'tempfile'

# tapsoob deps
require_relative 'config'

Tapsoob::Config.tapsoob_database_url = ENV['TAPSOOB_DATABASE_URL'] || begin
  # this is dirty but it solves a weird problem where the tempfile disappears mid-process
  #require ((RUBY_PLATFORM =~ /java/).nil? ? 'sqlite3' : 'jdbc-sqlite3')
  $__taps_database = Tempfile.new('tapsoob.db')
  $__taps_database.open()
  "sqlite://#{$__taps_database.path}"
end

# CLI modules
require_relative 'cli/schema'
require_relative 'cli/data'
require_relative 'cli/root'
