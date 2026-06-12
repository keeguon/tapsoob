require 'spec_helper'

RSpec.describe 'SQLite round-trip', :integration do
  before(:all) do
    @src_url = DbHelpers.adapt_url('sqlite://tmp/tapsoob_sqlite_src.db')
    @dst_url = DbHelpers.adapt_url('sqlite://tmp/tapsoob_sqlite_dst.db')

    FileUtils.mkdir_p('tmp')
    File.delete('tmp/tapsoob_sqlite_src.db') rescue nil
    File.delete('tmp/tapsoob_sqlite_dst.db') rescue nil

    @src_db = DbHelpers.connect(@src_url)
    @dst_db = DbHelpers.connect(@dst_url)

    Fixtures.create_tables(@src_db)
    Fixtures.seed(@src_db)
  end

  before(:each) do
    Fixtures.drop_tables(@dst_db)
  end

  after(:all) do
    Fixtures.drop_tables(@src_db)
    Fixtures.drop_tables(@dst_db)
    DbHelpers.disconnect_all
    File.delete('tmp/tapsoob_sqlite_src.db') rescue nil
    File.delete('tmp/tapsoob_sqlite_dst.db') rescue nil
  end

  include_examples 'a complete round-trip'

  # ── SQLite-specific edge cases ───────────────────────────────────────────────

  context 'with --discard-identity' do
    let(:discard_dir) { Dir.mktmpdir }
    after { FileUtils.rm_rf(discard_dir) }

    it 'inserts rows without the id column' do
      pull(src_url, discard_dir)

      dst_db.drop_table(:orders, if_exists: true)
      dst_db.drop_table(:users, if_exists: true)
      dst_db.create_table(:users) do
        primary_key :id
        String   :name,       size: 100
        String   :email,      size: 255
        String   :locale,     size: 10
        Integer  :age
        Date     :birthday
        DateTime :created_at
        DateTime :updated_at
      end

      push(dst_url, discard_dir, :"discard-identity" => true, schema: false)
      expect(dst_db[:users].count).to eq(src_db[:users].count)
    end
  end

  context 'with --tables filter' do
    let(:filtered_dir) { Dir.mktmpdir }
    after { FileUtils.rm_rf(filtered_dir) }

    it 'only pulls the specified tables' do
      pull(src_url, filtered_dir, tables: ['users', 'orders'])
      schema_files = Dir.glob(File.join(filtered_dir, 'schemas', '*.rb'))
        .map { |f| File.basename(f, '.rb') }
      expect(schema_files).to match_array(%w[users orders])
    end
  end

  context 'with --exclude-tables' do
    let(:excl_dir) { Dir.mktmpdir }
    after { FileUtils.rm_rf(excl_dir) }

    it 'excludes specified tables from the pull' do
      pull(src_url, excl_dir, exclude_tables: ['large_table'])
      expect(File).not_to exist(File.join(excl_dir, 'schemas', 'large_table.rb'))
    end
  end

  context 'with custom chunksize' do
    let(:chunk_dir) { Dir.mktmpdir }
    after { FileUtils.rm_rf(chunk_dir) }

    [10, 50, 500, 5000].each do |cs|
      it "round-trips with chunksize #{cs}" do
        round_trip(src_url, dst_url, chunk_dir, default_chunksize: cs)
        expect(dst_db[:users].count).to eq(src_db[:users].count)
      end
    end
  end

  context 'with empty tables' do
    let(:empty_dir) { Dir.mktmpdir }
    after { FileUtils.rm_rf(empty_dir) }

    it 'handles a completely empty table gracefully' do
      empty_src_url = DbHelpers.adapt_url('sqlite://tmp/tapsoob_sqlite_empty.db')
      empty_dst_url = DbHelpers.adapt_url('sqlite://tmp/tapsoob_sqlite_empty_dst.db')
      empty_src_db  = DbHelpers.connect(empty_src_url)
      empty_dst_db  = DbHelpers.connect(empty_dst_url)

      empty_src_db.create_table!(:empty_table) { primary_key :id; String :name, size: 50 }

      round_trip(empty_src_url, empty_dst_url, empty_dir)

      expect(empty_dst_db.table_exists?(:empty_table)).to be true
      expect(empty_dst_db[:empty_table].count).to eq(0)
    ensure
      DbHelpers.disconnect_all
      File.delete('tmp/tapsoob_sqlite_empty.db')     rescue nil
      File.delete('tmp/tapsoob_sqlite_empty_dst.db') rescue nil
      # Reconnect suite DBs after disconnect_all
      @src_db = DbHelpers.connect(@src_url)
      @dst_db = DbHelpers.connect(@dst_url)
    end
  end
end
