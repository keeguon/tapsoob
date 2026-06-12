require 'spec_helper'

# MySQL integration suite.
# In CI: supplied by the mysql service container via MYSQL_* env vars.
# Locally:
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
    skip 'MySQL not available' unless mysql_available?

    host     = ENV.fetch('MYSQL_HOST', 'mysql')
    port     = ENV.fetch('MYSQL_PORT', '3306')
    user     = ENV.fetch('MYSQL_USER', 'root')
    password = ENV.fetch('MYSQL_PASSWORD', 'root')

    Sequel.connect("mysql2://#{user}:#{password}@#{host}:#{port}/mysql") do |db|
      db.run("CREATE DATABASE IF NOT EXISTS tapsoob_src")
      db.run("CREATE DATABASE IF NOT EXISTS tapsoob_dst")
    end

    @src_url = DbHelpers.adapt_url(
      ENV.fetch('SRC_DATABASE_URL', "mysql2://#{user}:#{password}@#{host}:#{port}/tapsoob_src"))
    @dst_url = DbHelpers.adapt_url(
      ENV.fetch('DST_DATABASE_URL', "mysql2://#{user}:#{password}@#{host}:#{port}/tapsoob_dst"))

    @src_db = DbHelpers.connect(@src_url)
    @dst_db = DbHelpers.connect(@dst_url)

    Fixtures.create_tables(@src_db)
    Fixtures.seed(@src_db)
  end

  before(:each) do
    Fixtures.drop_tables(@dst_db) if @dst_db
  end

  after(:all) do
    Fixtures.drop_tables(@src_db)   if @src_db
    Fixtures.drop_tables(@dst_db)   if @dst_db
    DbHelpers.disconnect_all
  end

  include_examples 'a complete round-trip'
  include_examples 'a parallel round-trip', workers: 2
  include_examples 'a parallel round-trip', workers: 4

  context 'with SET foreign_key_checks behavior' do
    it 'loads schema with FK relationships intact' do
      pull(src_url, dump_dir)
      push(dst_url, dump_dir)
      expect(dst_db.table_exists?(:orders)).to be true
      expect(dst_db.table_exists?(:users)).to be true
    end
  end

  context 'with large BLOB payloads' do
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
      expect { round_trip(src_url, dst_url, dump_dir) }.not_to raise_error
    end
  end
end
