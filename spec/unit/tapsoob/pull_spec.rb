require 'spec_helper'
require 'tapsoob/operation/pull'

RSpec.describe Tapsoob::Operation::Pull do
  let(:db)       { seeded_sqlite_db }
  let(:dump_dir) { Dir.mktmpdir("tapsoob_pull_") }

  after do
    db.disconnect
    FileUtils.rm_rf(dump_dir)
  end

  # ── initialize_dump_directory ────────────────────────────────────────────────

  describe '#initialize_dump_directory' do
    it 'creates data, schemas, and indexes subdirectories' do
      build_pull(db, dump_dir).initialize_dump_directory
      %w[data schemas indexes].each do |sub|
        expect(File.directory?(File.join(dump_dir, sub))).to be true
      end
    end

    it 'removes table_order.txt if present' do
      order_file = File.join(dump_dir, "table_order.txt")
      File.write(order_file, "users\n")
      build_pull(db, dump_dir).initialize_dump_directory
      expect(File.exist?(order_file)).to be false
    end

    it 'cleans existing subdirectory contents' do
      stale = File.join(dump_dir, "data", "old_table.json")
      FileUtils.mkdir_p(File.dirname(stale))
      File.write(stale, "stale")
      build_pull(db, dump_dir).initialize_dump_directory
      expect(File.exist?(stale)).to be false
    end
  end

  # ── fetch_tables_info ────────────────────────────────────────────────────────

  describe '#fetch_tables_info' do
    it 'returns a hash of table_name => count (symbol keys from Sequel)' do
      expect(build_pull(db, dump_dir).fetch_tables_info).to include(:users => 5, :widgets => 3)
    end

    it 'applies table_filter when provided' do
      info = build_pull(db, dump_dir, tables: ["users"]).fetch_tables_info
      expect(info.keys.map(&:to_s)).to contain_exactly("users")
    end

    it 'applies exclude_tables when provided' do
      info = build_pull(db, dump_dir, exclude_tables: ["widgets"]).fetch_tables_info
      expect(info.keys.map(&:to_s)).not_to include("widgets")
    end
  end

  # ── tables / record_count ────────────────────────────────────────────────────

  describe '#tables' do
    it 'excludes already-completed tables' do
      op = build_pull(db, dump_dir)
      op.opts[:completed_tables] = ["users"]
      expect(op.tables.keys).not_to include("users")
    end
  end

  describe '#record_count' do
    it 'sums all table counts' do
      expect(build_pull(db, dump_dir).record_count).to eq(8)
    end
  end

  # ── pull_schema ──────────────────────────────────────────────────────────────

  describe '#pull_schema' do
    before { build_pull(db, dump_dir).initialize_dump_directory }

    it 'writes a schema file for each table' do
      op = build_pull(db, dump_dir)
      op.pull_schema
      %w[users widgets].each do |t|
        schema_file = File.join(dump_dir, "schemas", "#{t}.rb")
        expect(File.exist?(schema_file)).to be true
        expect(File.size(schema_file)).to be > 0
      end
    end

    it 'writes table_order.txt listing all tables' do
      op = build_pull(db, dump_dir)
      op.pull_schema
      content = File.read(File.join(dump_dir, "table_order.txt"))
      %w[users widgets].each { |t| expect(content).to include(t) }
    end
  end

  # ── pull_data_serial ─────────────────────────────────────────────────────────

  describe '#pull_data_serial' do
    before do
      op = build_pull(db, dump_dir)
      op.initialize_dump_directory
      op.pull_schema
    end

    it 'writes NDJSON data files for each table' do
      op = build_pull(db, dump_dir)
      op.pull_data_serial
      %w[users widgets].each do |t|
        data_file = File.join(dump_dir, "data", "#{t}.json")
        expect(File.exist?(data_file)).to be true
        parsed = JSON.parse(File.readlines(data_file).first.strip)
        expect(parsed).to have_key("data")
      end
    end

    it 'marks tables as completed after writing' do
      op = build_pull(db, dump_dir)
      op.pull_data_serial
      expect(op.completed_tables).to include("users", "widgets")
    end
  end

  # ── save_table_order / load_table_order ──────────────────────────────────────

  describe '#save_table_order / #load_table_order' do
    it 'round-trips table names through the order file' do
      op = build_pull(db, dump_dir)
      op.save_table_order(["users", "widgets"])
      expect(op.load_table_order).to eq(["users", "widgets"])
    end
  end

  # ── Base#to_hash ─────────────────────────────────────────────────────────────

  describe '#to_hash (base fields)' do
    it 'includes klass and database_url keys' do
      op   = build_pull(db, dump_dir)
      hash = Tapsoob::Operation::Base.instance_method(:to_hash).bind(op).call
      expect(hash).to have_key(:klass)
      expect(hash).to have_key(:database_url)
    end
  end

  # ── apply_table_filter ───────────────────────────────────────────────────────

  describe '#apply_table_filter' do
    it 'passes all tables when no filter is set' do
      op    = build_pull(db, dump_dir)
      input = { "users" => 5, "widgets" => 3 }
      expect(op.apply_table_filter(input)).to eq(input)
    end

    it 'selects only filtered tables' do
      op = build_pull(db, dump_dir, tables: ["users"])
      expect(op.apply_table_filter({ "users" => 5, "widgets" => 3 })).to eq("users" => 5)
    end
  end

  # ── pull_data (routing) ──────────────────────────────────────────────────────

  describe '#pull_data' do
    before do
      op = build_pull(db, dump_dir)
      op.initialize_dump_directory
      op.pull_schema
    end

    it 'runs serial path by default' do
      op = build_pull(db, dump_dir)
      op.pull_data
      expect(File.exist?(File.join(dump_dir, "data", "users.json"))).to be true
    end

    it 'runs parallel path when parallel > 1' do
      op = build_pull(db, dump_dir, parallel: 2, no_split: true)
      op.pull_data
      expect(File.exist?(File.join(dump_dir, "data", "users.json"))).to be true
    end
  end

  # ── pull_reset_sequences ─────────────────────────────────────────────────────

  describe '#pull_reset_sequences' do
    it 'runs without error on SQLite' do
      op = build_pull(db, dump_dir)
      expect { op.pull_reset_sequences }.not_to raise_error
    end
  end

  # ── pull_indexes ─────────────────────────────────────────────────────────────

  describe '#pull_indexes' do
    before do
      op = build_pull(db, dump_dir)
      op.initialize_dump_directory
    end

    it 'writes index files to the indexes directory' do
      op = build_pull(db, dump_dir)
      op.pull_indexes
      expect(File.directory?(File.join(dump_dir, "indexes"))).to be true
    end
  end

  # ── store_session ────────────────────────────────────────────────────────────

  describe '#store_session' do
    it 'writes a .dat session file to the current directory' do
      op = build_pull(db, dump_dir)
      session_file = nil
      begin
        op.store_session
        files = Dir.glob("pull_*.dat")
        session_file = files.first
        expect(session_file).not_to be_nil
        data = JSON.parse(File.read(session_file))
        expect(data).to have_key("database_url")
      ensure
        File.delete(session_file) if session_file && File.exist?(session_file)
      end
    end
  end

  # ── pull_partial_data ────────────────────────────────────────────────────────

  describe '#pull_partial_data' do
    before do
      op = build_pull(db, dump_dir)
      op.initialize_dump_directory
      op.pull_schema
    end

    it 'returns early when stream_state is empty' do
      op = build_pull(db, dump_dir)
      expect { op.pull_partial_data }.not_to raise_error
    end
  end
end
