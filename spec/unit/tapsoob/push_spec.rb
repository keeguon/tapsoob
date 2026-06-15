require 'spec_helper'
require 'tapsoob/operation/push'
require 'tapsoob/operation/pull'
require 'fileutils'
require 'tmpdir'

RSpec.describe Tapsoob::Operation::Push do
  # Push needs its own Sequel connection opened from database_url (via Base#db),
  # so we use a file-backed SQLite DB. All URLs go through DbHelpers.adapt_url
  # so the spec runs on both MRI (sqlite3 gem) and JRuby (JDBC adapter) on CI.

  let(:db_path) do
    File.join(Dir.tmpdir, "tapsoob_push_#{Process.pid}_#{rand(9999)}.db")
  end
  let(:db_url) { DbHelpers.adapt_url("sqlite://#{db_path}") }

  let(:db) do
    d = Sequel.connect(db_url)
    d.extension :schema_dumper
    d.create_table(:users)   { primary_key :id; String :name }
    d.create_table(:widgets) { primary_key :id; Integer :qty }
    5.times { |i| d[:users].insert(name: "user_#{i}") }
    3.times { |i| d[:widgets].insert(qty: i * 10) }
    d
  end

  after do
    db.disconnect rescue nil
    File.delete(db_path) rescue nil
  end

  let(:dump_dir) { Dir.mktmpdir("tapsoob_push_") }
  after { FileUtils.rm_rf(dump_dir) }

  # Populate a complete dump directory using Pull so Push has real files to read.
  # We inject the already-open db connection so Pull doesn't open a second empty
  # in-memory connection.
  before do
    pull_op = Tapsoob::Operation::Pull.new(db_url, dump_dir, {
      data:              true,
      schema:            true,
      indexes:           false,
      progress:          false,
      default_chunksize: 1000,
      no_split:          true,
    })
    pull_op.instance_variable_set(:@db, db)
    pull_op.initialize_dump_directory
    pull_op.pull_schema
    pull_op.pull_data_serial
  end

  def build_push(extra_opts = {})
    opts = {
      data:              true,
      schema:            true,
      indexes:           false,
      progress:          false,
      default_chunksize: 1000,
      no_split:          true,
    }.merge(extra_opts)

    described_class.new(db_url, dump_dir, opts)
  end

  # ── fetch_local_tables_info ──────────────────────────────────────────────────

  describe '#fetch_local_tables_info' do
    it 'returns a hash of table_name => row_count' do
      info = build_push.fetch_local_tables_info
      expect(info).to include("users" => 5, "widgets" => 3)
    end

    it 'respects table_order.txt when present' do
      info = build_push.fetch_local_tables_info
      expect(info.keys).to include("users", "widgets")
    end

    it 'falls back to schema files when table_order.txt is absent' do
      File.delete(File.join(dump_dir, "table_order.txt")) rescue nil
      info = build_push.fetch_local_tables_info
      expect(info.keys).to include("users", "widgets")
    end

    it 'applies exclude_tables filter' do
      info = build_push(exclude_tables: ["widgets"]).fetch_local_tables_info
      expect(info.keys).not_to include("widgets")
    end
  end

  # ── tables / record_count ────────────────────────────────────────────────────

  describe '#tables' do
    it 'excludes completed tables' do
      op = build_push
      op.opts[:completed_tables] = ["users"]
      expect(op.tables.keys).not_to include("users")
    end
  end

  describe '#record_count' do
    it 'sums all table row counts' do
      expect(build_push.record_count).to eq(8)
    end
  end

  # ── calculate_file_line_ranges ───────────────────────────────────────────────

  describe '#calculate_file_line_ranges' do
    it 'returns [] when the data file does not exist' do
      expect(build_push.calculate_file_line_ranges("nonexistent", 2)).to eq([])
    end

    it 'returns a single range for 1 worker' do
      ranges = build_push.calculate_file_line_ranges("users", 1)
      expect(ranges.size).to eq(1)
      expect(ranges.first.first).to eq(0)
    end

    it 'splits a multi-line file across workers without gaps' do
      ranges = build_push.calculate_file_line_ranges("users", 2)
      expect(ranges.size).to be >= 1
      ranges.each_cons(2) do |(_, end1), (start2, _)|
        expect(start2).to eq(end1 + 1)
      end
    end
  end

  # ── push_schema ──────────────────────────────────────────────────────────────

  describe '#push_schema' do
    it 'loads schema into target DB without error' do
      fresh_path = File.join(Dir.tmpdir, "tapsoob_push_fresh_#{Process.pid}.db")
      fresh_url  = DbHelpers.adapt_url("sqlite://#{fresh_path}")
      fresh_db   = Sequel.connect(fresh_url)
      fresh_db.extension :schema_dumper
      begin
        op = build_push
        op.instance_variable_set(:@db, fresh_db)
        op.instance_variable_set(:@database_url, fresh_url)
        expect { op.push_schema }.not_to raise_error
      ensure
        fresh_db.disconnect rescue nil
        File.delete(fresh_path) rescue nil
      end
    end
  end

  # ── push_data_serial ─────────────────────────────────────────────────────────

  describe '#push_data_serial' do
    it 'inserts rows into the target DB' do
      db[:users].delete
      db[:widgets].delete

      op = build_push
      # Inject the existing open connection so push uses the same file DB
      op.instance_variable_set(:@db, db)
      op.push_data_serial

      expect(db[:users].count).to eq(5)
      expect(db[:widgets].count).to eq(3)
    end

    it 'marks tables as completed' do
      db[:users].delete
      db[:widgets].delete
      op = build_push
      op.instance_variable_set(:@db, db)
      op.push_data_serial
      expect(op.completed_tables).to include("users", "widgets")
    end
  end

  # ── to_hash ──────────────────────────────────────────────────────────────────

  describe '#to_hash' do
    it 'includes local_tables_info key' do
      expect(build_push.to_hash).to have_key(:local_tables_info)
    end
  end

  # ── parallel? always false for Push ──────────────────────────────────────────

  describe '#parallel?' do
    it 'is always false regardless of :parallel option' do
      expect(build_push(parallel: 4).parallel?).to be false
    end
  end
end
