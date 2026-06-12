require 'simplecov'
SimpleCov.start do
  add_filter '/spec/'
  add_group 'Operation',   'lib/tapsoob/operation'
  add_group 'DataStream',  'lib/tapsoob/data_stream'
  add_group 'CLI',         'lib/tapsoob/cli'
end

require 'tapsoob'
require 'sequel'
require 'fileutils'
require 'tmpdir'

Dir[File.join(__dir__, 'support', '**', '*.rb')].sort.each { |f| require f }

# ── JRuby-aware SQLite helpers ───────────────────────────────────────────────
# Use these everywhere instead of Sequel.sqlite / 'sqlite::memory:' so the
# unit specs run identically under MRI and JRuby.

def sqlite_memory_url
  DbHelpers.adapt_url('sqlite::memory:')
end

def connect_sqlite
  db = Sequel.connect(sqlite_memory_url)
  db.extension :schema_dumper
  db
end

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.filter_run_when_matching :focus
  config.disable_monkey_patching!
  config.order = :random
  Kernel.srand config.seed

  # Integration/system tests require a real DB — skip unless env vars are set.
  config.filter_run_excluding :integration unless ENV['INTEGRATION_TESTS'] || ENV['SRC_DATABASE_URL']
  config.filter_run_excluding :system      unless ENV['SYSTEM_TESTS']      || ENV['SRC_DATABASE_URL']

  config.include DbHelpers,      :integration
  config.include DbHelpers,      :system
  config.include RoundTripHelper, :integration
  config.include RoundTripHelper, :system
end
