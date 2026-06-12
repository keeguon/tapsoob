require 'spec_helper'

# PostgreSQL integration suite.
# Requires SRC_DATABASE_URL / DST_DATABASE_URL pointing at postgres:// connections,
# OR set POSTGRES_HOST / POSTGRES_PORT / POSTGRES_USER / POSTGRES_PASSWORD env vars.
#
# In CI these are supplied by the postgres service container; locally you can run:
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
    unless postgres_available?
      skip 'PostgreSQL not available – set POSTGRES_HOST/USER/PASSWORD or SRC_DATABASE_URL'
    end

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
  end

  let(:host)     { ENV.fetch('POSTGRES_HOST', 'postgres') }
  let(:port)     { ENV.fetch('POSTGRES_PORT', '5432') }
  let(:user)     { ENV.fetch('POSTGRES_USER', 'postgres') }
  let(:password) { ENV.fetch('POSTGRES_PASSWORD', 'postgres') }

  let(:src_url) do
    ENV.fetch('SRC_DATABASE_URL',
      "postgres://#{user}:#{password}@#{host}:#{port}/tapsoob_src")
  end
  let(:dst_url) do
    ENV.fetch('DST_DATABASE_URL',
      "postgres://#{user}:#{password}@#{host}:#{port}/tapsoob_dst")
  end

  before(:all) do
    Fixtures.create_tables(src_db)
    Fixtures.seed(src_db)
  end

  after(:all) do
    Fixtures.drop_tables(src_db)
    Fixtures.drop_tables(dst_db)
    DbHelpers.disconnect_all
  end

  include_examples 'a complete round-trip'
  include_examples 'a parallel round-trip', workers: 2
  include_examples 'a parallel round-trip', workers: 4

  # ── PostgreSQL-specific edge cases ───────────────────────────────────────────

  context 'with sequence reset' do
    it 'sequences are reset after push so inserts work' do
      round_trip(src_url, dst_url, dump_dir)
      # If sequences were not reset, this insert would fail on PK conflict
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
