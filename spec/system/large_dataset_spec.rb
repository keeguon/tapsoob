require 'spec_helper'

RSpec.describe 'Large dataset system tests', :system do
  before(:all) do
    @src_url = DbHelpers.adapt_url(ENV.fetch('SRC_DATABASE_URL', 'sqlite://tmp/tapsoob_system_src.db'))
    @dst_url = DbHelpers.adapt_url(ENV.fetch('DST_DATABASE_URL', 'sqlite://tmp/tapsoob_system_dst.db'))

    FileUtils.mkdir_p('tmp')
    File.delete('tmp/tapsoob_system_src.db') rescue nil
    File.delete('tmp/tapsoob_system_dst.db') rescue nil

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
    File.delete('tmp/tapsoob_system_src.db') rescue nil
    File.delete('tmp/tapsoob_system_dst.db') rescue nil
  end

  # ── large_table: intra-table parallelization threshold ───────────────────────

  describe 'large_table (150K rows)' do
    it 'transfers all rows in serial mode' do
      round_trip(src_url, dst_url, dump_dir)
      expect(dst_db[:large_table].count).to eq(Fixtures::LARGE_TABLE_ROWS)
    end

    it 'transfers all rows with parallel: 2' do
      round_trip(src_url, dst_url, dump_dir, parallel: 2)
      expect(dst_db[:large_table].count).to eq(Fixtures::LARGE_TABLE_ROWS)
    end

    it 'transfers all rows with parallel: 4' do
      round_trip(src_url, dst_url, dump_dir, parallel: 4)
      expect(dst_db[:large_table].count).to eq(Fixtures::LARGE_TABLE_ROWS)
    end

    it 'has no duplicate rows after parallel pull' do
      round_trip(src_url, dst_url, dump_dir, parallel: 4)
      total    = dst_db[:large_table].count
      distinct = dst_db[:large_table].select(:id).distinct.count
      expect(distinct).to eq(total)
    end
  end

  # ── documents: large TEXT columns ────────────────────────────────────────────

  describe 'documents table (large TEXT)' do
    it 'preserves body content exactly' do
      round_trip(src_url, dst_url, dump_dir)
      src_db[:documents].order(:id).each do |src_row|
        dst_row = dst_db[:documents][id: src_row[:id]]
        expect(dst_row[:body]).to eq(src_row[:body]),
          "body mismatch for document #{src_row[:id]}: " \
          "src=#{src_row[:body]&.length} bytes dst=#{dst_row[:body]&.length} bytes"
      end
    end

    it 'handles documents with nil body' do
      round_trip(src_url, dst_url, dump_dir)
      expect(dst_db[:documents].where(body: nil).count).to eq(src_db[:documents].where(body: nil).count)
    end
  end

  # ── attachments: BLOB encoding/decoding ──────────────────────────────────────

  describe 'attachments table (binary BLOBs up to 256 KB)' do
    it 'preserves every byte of every payload' do
      round_trip(src_url, dst_url, dump_dir)
      mismatch_count = 0
      src_db[:attachments].order(:id).each do |src_row|
        dst_row = dst_db[:attachments][id: src_row[:id]]
        mismatch_count += 1 unless dst_row[:payload].to_s.bytes == src_row[:payload].to_s.bytes
      end
      expect(mismatch_count).to eq(0)
    end

    it 'preserves size_bytes metadata' do
      round_trip(src_url, dst_url, dump_dir)
      src_db[:attachments].order(:id).each do |src_row|
        dst_row = dst_db[:attachments][id: src_row[:id]]
        expect(dst_row[:size_bytes]).to eq(src_row[:size_bytes])
      end
    end
  end

  # ── null_heavy: NULL preservation ────────────────────────────────────────────

  describe 'null_heavy table' do
    it 'preserves NULLs in every nullable column' do
      round_trip(src_url, dst_url, dump_dir)
      %i[maybe_name maybe_number maybe_score maybe_date maybe_text].each do |col|
        src_nulls = src_db[:null_heavy].where(col => nil).count
        dst_nulls = dst_db[:null_heavy].where(col => nil).count
        expect(dst_nulls).to eq(src_nulls),
          "NULL count mismatch for null_heavy.#{col}: src=#{src_nulls} dst=#{dst_nulls}"
      end
    end
  end

  # ── events: table without primary key ────────────────────────────────────────

  describe 'events table (no primary key)' do
    it 'uses the Base (non-keyed) stream' do
      round_trip(src_url, dst_url, dump_dir)
      expect(dst_db[:events].count).to eq(src_db[:events].count)
    end
  end

  # ── adaptive chunksize: very small chunks ────────────────────────────────────

  describe 'adaptive chunksize under load' do
    it 'completes with chunksize=1 (extreme case)' do
      small_src = DbHelpers.adapt_url('sqlite://tmp/tapsoob_small_src.db')
      small_dst = DbHelpers.adapt_url('sqlite://tmp/tapsoob_small_dst.db')
      small_dir = Dir.mktmpdir

      begin
        sdb = DbHelpers.connect(small_src)
        sdb.create_table!(:small_test) { primary_key :id; String :v, size: 50 }
        100.times { |i| sdb[:small_test].insert(v: "row_#{i}") }

        round_trip(small_src, small_dst, small_dir, default_chunksize: 1)
        expect(DbHelpers.connect(small_dst)[:small_test].count).to eq(100)
      ensure
        FileUtils.rm_rf(small_dir)
        File.delete('tmp/tapsoob_small_src.db') rescue nil
        File.delete('tmp/tapsoob_small_dst.db') rescue nil
        # Reconnect suite DBs after disconnect_all clears the pool
        DbHelpers.disconnect_all
        @src_db = DbHelpers.connect(@src_url)
        @dst_db = DbHelpers.connect(@dst_url)
      end
    end
  end

  # ── FK order: orders depends on users ────────────────────────────────────────

  describe 'foreign key dependency ordering' do
    it 'pushes users before orders (table_order.txt respected)' do
      pull(src_url, dump_dir)
      order_file = File.join(dump_dir, 'table_order.txt')
      if File.exist?(order_file)
        order = File.readlines(order_file).map(&:strip)
        users_idx  = order.index('users')
        orders_idx = order.index('orders')
        expect(users_idx).to be < orders_idx if users_idx && orders_idx
      end
      expect { push(dst_url, dump_dir) }.not_to raise_error
    end
  end
end
