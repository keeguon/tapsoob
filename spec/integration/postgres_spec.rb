require 'spec_helper'

# PostgreSQL integration suite.
# In CI: supplied by the postgres service container via POSTGRES_* env vars.
# Locally:
#   SRC_DATABASE_URL=postgres://postgres:postgres@127.0.0.1/tapsoob_src \
#   DST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1/tapsoob_dst \
#   bundle exec rspec spec/integration/postgres_spec.rb

def postgres_available?
  return false unless defined?(PG)
  host     = ENV.fetch('POSTGRES_HOST', 'postgres')
  port     = ENV.fetch('POSTGRES_PORT', '5432')
  user     = ENV.fetch('POSTGRES_USER', 'postgres')
  password = ENV.fetch('POSTGRES_PASSWORD', 'postgres')
  Sequel.connect("postgres://#{user}:#{password}@#{host}:#{port}/postgres").disconnect
  true
rescue StandardError
  false
end

RSpec.describe 'PostgreSQL round-trip', :integration do
  before(:all) do
    skip 'PostgreSQL not available' unless postgres_available?

    host     = ENV.fetch('POSTGRES_HOST', 'postgres')
    port     = ENV.fetch('POSTGRES_PORT', '5432')
    user     = ENV.fetch('POSTGRES_USER', 'postgres')
    password = ENV.fetch('POSTGRES_PASSWORD', 'postgres')

    Sequel.connect("postgres://#{user}:#{password}@#{host}:#{port}/postgres") do |db|
      ['tapsoob_src', 'tapsoob_dst'].each do |dbname|
        db.run("DROP DATABASE IF EXISTS #{dbname}")
        db.run("CREATE DATABASE #{dbname}")
      end
    end

    @src_url = DbHelpers.adapt_url(
      ENV.fetch('SRC_DATABASE_URL', "postgres://#{user}:#{password}@#{host}:#{port}/tapsoob_src"))
    @dst_url = DbHelpers.adapt_url(
      ENV.fetch('DST_DATABASE_URL', "postgres://#{user}:#{password}@#{host}:#{port}/tapsoob_dst"))

    @src_db = DbHelpers.connect(@src_url)
    @dst_db = DbHelpers.connect(@dst_url)

    Fixtures.create_tables(@src_db)
    Fixtures.seed(@src_db)
  end

  after(:all) do
    Fixtures.drop_tables(@src_db)   if @src_db
    Fixtures.drop_tables(@dst_db)   if @dst_db
    DbHelpers.disconnect_all
  end

  include_examples 'a complete round-trip'
  include_examples 'a parallel round-trip', workers: 2
  include_examples 'a parallel round-trip', workers: 4

  context 'with sequence reset' do
    it 'sequences are reset after push so inserts work' do
      round_trip(src_url, dst_url, dump_dir)
      expect {
        dst_db[:users].insert(
          name:       'PostPush User',
          email:      "postpush_#{rand(9999)}@example.com",
          created_at: Time.now.strftime('%Y-%m-%d %H:%M:%S'),
          updated_at: Time.now.strftime('%Y-%m-%d %H:%M:%S')
        )
      }.not_to raise_error
    end
  end

  context 'with bytea BLOB columns' do
    it 'transfers bytea payloads with correct byte content' do
      round_trip(src_url, dst_url, dump_dir)
      src_db[:attachments].order(:id).each do |src_row|
        dst_row = dst_db[:attachments][id: src_row[:id]]
        expect(dst_row[:payload].to_s.bytes).to eq(src_row[:payload].to_s.bytes)
      end
    end
  end

  context 'with --indexes-first' do
    let(:idx_dir) { Dir.mktmpdir }
    after { FileUtils.rm_rf(idx_dir) }

    it 'creates indexes before loading data without error' do
      pull(src_url, idx_dir, indexes_first: true)
      expect { push(dst_url, idx_dir, indexes_first: true) }.not_to raise_error
      expect_same_counts(src_db, dst_db)
    end
  end
end
