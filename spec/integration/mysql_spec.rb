require 'spec_helper'

# MySQL integration suite.
# Requires SRC_DATABASE_URL / DST_DATABASE_URL pointing at mysql2:// connections,
# OR set MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD env vars.
#
# In CI these are supplied by the mysql service container; locally you can run:
#   SRC_DATABASE_URL=mysql2://root:root@127.0.0.1/tapsoob_src \
#   DST_DATABASE_URL=mysql2://root:root@127.0.0.1/tapsoob_dst \
#   bundle exec rspec spec/integration/mysql_spec.rb

def mysql_available?
  return false unless defined?(Mysql2)
  host     = ENV.fetch('MYSQL_HOST', 'mysql')
  port     = ENV.fetch('MYSQL_PORT', '3306')
  user     = ENV.fetch('MYSQL_USER', 'root')
  password = ENV.fetch('MYSQL_PASSWORD', 'root')
  Sequel.connect("mysql2://#{user}:#{password}@#{host}:#{port}/mysql").disconnect
  true
rescue StandardError
  false
end

RSpec.describe 'MySQL round-trip', :integration do
  before(:all) do
    unless mysql_available?
      skip 'MySQL not available – set MYSQL_HOST/USER/PASSWORD or SRC_DATABASE_URL'
    end

    host     = ENV.fetch('MYSQL_HOST', 'mysql')
    port     = ENV.fetch('MYSQL_PORT', '3306')
    user     = ENV.fetch('MYSQL_USER', 'root')
    password = ENV.fetch('MYSQL_PASSWORD', 'root')

    Sequel.connect("mysql2://#{user}:#{password}@#{host}:#{port}/mysql") do |db|
      db.run("CREATE DATABASE IF NOT EXISTS tapsoob_src")
      db.run("CREATE DATABASE IF NOT EXISTS tapsoob_dst")
    end
  end

  let(:host)     { ENV.fetch('MYSQL_HOST', 'mysql') }
  let(:port)     { ENV.fetch('MYSQL_PORT', '3306') }
  let(:user)     { ENV.fetch('MYSQL_USER', 'root') }
  let(:password) { ENV.fetch('MYSQL_PASSWORD', 'root') }

  let(:src_url) do
    ENV.fetch('SRC_DATABASE_URL',
      "mysql2://#{user}:#{password}@#{host}:#{port}/tapsoob_src")
  end
  let(:dst_url) do
    ENV.fetch('DST_DATABASE_URL',
      "mysql2://#{user}:#{password}@#{host}:#{port}/tapsoob_dst")
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

  # ── MySQL-specific edge cases ────────────────────────────────────────────────

  context 'with SET foreign_key_checks behavior' do
    it 'loads schema with FK relationships intact' do
      pull(src_url, dump_dir)
      push(dst_url, dump_dir)
      expect(dst_db.table_exists?(:orders)).to be true
      # orders has a FK on users – both must exist
      expect(dst_db.table_exists?(:users)).to be true
    end
  end

  context 'with large BLOB payloads (> 1 MB implied by packet limits)' do
    let(:blob_dir) { Dir.mktmpdir }
    after { FileUtils.rm_rf(blob_dir) }

    it 'transfers all attachment blobs correctly' do
      pull(src_url, blob_dir)
      push(dst_url, blob_dir)

      src_db[:attachments].order(:id).each do |src_row|
        dst_row = dst_db[:attachments][id: src_row[:id]]
        expect(dst_row[:payload].to_s.bytes).to eq(src_row[:payload].to_s.bytes)
      end
    end
  end

  context 'with invalid date handling' do
    it 'tolerates 0000-00-00 MySQL dates without raising' do
      # MySQL can produce invalid dates that Sequel must convert to nil
      # This exercises the Sequel::MySQL.convert_invalid_date_time = :nil path
      expect {
        round_trip(src_url, dst_url, dump_dir)
      }.not_to raise_error
    end
  end
end
