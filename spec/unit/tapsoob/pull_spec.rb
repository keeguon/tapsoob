require 'spec_helper'
require 'tapsoob/operation/pull'
require 'fileutils'
require 'tmpdir'

RSpec.describe Tapsoob::Operation::Pull do
  let(:db) do
    d = connect_sqlite
    d.extension :schema_dumper
    d.create_table(:users)   { primary_key :id; String :name }
    d.create_table(:widgets) { primary_key :id; Integer :qty }
    5.times  { |i| d[:users].insert(name: "user_#{i}") }
    3.times  { |i| d[:widgets].insert(qty: i * 10) }
    d
  end
  after { db.disconnect }

  let(:dump_dir) { Dir.mktmpdir("tapsoob_pull_") }
  after { FileUtils.rm_rf(dump_dir) }

  def build_pull(extra_opts = {})
    url  = sqlite_memory_url
    opts = {
      data:              true,
      schema:            true,
      indexes:           false,
      progress:          false,
      default_chunksize: 1000,
      no_split:          true,
    }.merge(extra_opts)

    op = described_class.new(url, dump_dir, opts)
    # Inject our pre-seeded DB so we don't open a second empty memory connection
    op.instance_variable_set(:@db, db)
    op
  end

  # ── initialize_dump_directory ────────────────────────────────────────────────

  describe '#initialize_dump_directory' do
    it 'creates data, schemas, and indexes subdirectories' do
      build_pull.initialize_dump_directory
      %w[data schemas indexes].each do |sub|
        expect(File.directory?(File.join(dump_dir, sub))).to be true
      end
    end

    it 'removes table_order.txt if present' do
      order_file = File.join(dump_dir, "table_order.txt")
      FileUtils.mkdir_p(dump_dir)
      File.write(order_file, "users\n")
      build_pull.initialize_dump_directory
      expect(File.exist?(order_file)).to be false
    end

    it 'cleans existing subdirectory contents' do
      stale = File.join(dump_dir, "data", "old_table.json")
      FileUtils.mkdir_p(File.dirname(stale))
      File.write(stale, "stale")
      build_pull.initialize_dump_directory
      expect(File.exist?(stale)).to be false
    end
  end

  # ── fetch_tables_info ────────────────────────────────────────────────────────

  describe '#fetch_tables_info' do
    it 'returns a hash of table_name => count (symbol keys from Sequel)' do
      info = build_pull.fetch_tables_info
      # Sequel returns symbol keys for table names
      expect(info).to include(:users => 5, :widgets => 3)
    end

    it 'applies table_filter when provided' do
      op   = build_pull(tables: ["users"])
      info = op.fetch_tables_info
      expect(info.keys.map(&:to_s)).to contain_exactly("users")
    end

    it 'applies exclude_tables when provided' do
      op   = build_pull(exclude_tables: ["widgets"])
      info = op.fetch_tables_info
      expect(info.keys.map(&:to_s)).not_to include("widgets")
    end
  end

  # ── tables / record_count ────────────────────────────────────────────────────

  describe '#tables' do
    it 'excludes already-completed tables' do
      op = build_pull
      op.opts[:completed_tables] = ["users"]
      expect(op.tables.keys).not_to include("users")
    end
  end

  describe '#record_count' do
    it 'sums all table counts' do
      op = build_pull
      expect(op.record_count).to eq(8)
    end
  end

  # ── pull_schema ──────────────────────────────────────────────────────────────

  describe '#pull_schema' do
    before { build_pull.initialize_dump_directory }

    it 'writes a schema file for each table' do
      op = build_pull
      op.pull_schema
      %w[users widgets].each do |t|
        schema_file = File.join(dump_dir, "schemas", "#{t}.rb")
        expect(File.exist?(schema_file)).to be true
        expect(File.size(schema_file)).to be > 0
      end
    end

    it 'writes table_order.txt' do
      op = build_pull
      op.pull_schema
      order_file = File.join(dump_dir, "table_order.txt")
      expect(File.exist?(order_file)).to be true
      content = File.read(order_file)
      %w[users widgets].each { |t| expect(content).to include(t) }
    end
  end

  # ── pull_data_serial ─────────────────────────────────────────────────────────

  describe '#pull_data_serial' do
    before do
      op = build_pull
      op.initialize_dump_directory
      op.pull_schema
    end

    it 'writes NDJSON data files for each table' do
      op = build_pull
      op.pull_data_serial
      %w[users widgets].each do |t|
        data_file = File.join(dump_dir, "data", "#{t}.json")
        expect(File.exist?(data_file)).to be true
        line = File.readlines(data_file).first
        parsed = JSON.parse(line.strip)
        expect(parsed).to have_key("data")
      end
    end

    it 'marks tables as completed after writing' do
      op = build_pull
      op.pull_data_serial
      expect(op.completed_tables).to include("users", "widgets")
    end
  end

  # ── save_table_order / load_table_order ──────────────────────────────────────

  describe '#save_table_order / #load_table_order' do
    it 'round-trips table names through the order file' do
      op = build_pull
      op.save_table_order(["users", "widgets"])
      expect(op.load_table_order).to eq(["users", "widgets"])
    end
  end

  # ── Base#to_hash ─────────────────────────────────────────────────────────────

  describe '#to_hash (base fields)' do
    it 'includes klass and database_url keys' do
      op   = build_pull
      hash = Tapsoob::Operation::Base.instance_method(:to_hash).bind(op).call
      expect(hash).to have_key(:klass)
      expect(hash).to have_key(:database_url)
    end
  end

  # ── apply_table_filter ───────────────────────────────────────────────────────

  describe '#apply_table_filter' do
    it 'passes all tables when no filter is set' do
      op    = build_pull
      input = { "users" => 5, "widgets" => 3 }
      expect(op.apply_table_filter(input)).to eq(input)
    end

    it 'selects only filtered tables' do
      op = build_pull(tables: ["users"])
      expect(op.apply_table_filter({ "users" => 5, "widgets" => 3 })).to eq("users" => 5)
    end
  end
end
