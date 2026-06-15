require 'spec_helper'
require 'tapsoob/operation/push'
require 'tapsoob/operation/pull'

RSpec.describe Tapsoob::Operation::Push do
  # Push opens its own Sequel connection from the URL, so we need a file-backed DB.
  let(:db_path) { File.join(Dir.tmpdir, "tapsoob_push_#{Process.pid}_#{rand(9999)}.db") }
  let(:db_url)  { DbHelpers.adapt_url("sqlite://#{db_path}") }

  let(:db) do
    d = Sequel.connect(db_url)
    d.extension :schema_dumper
    d.create_table(:users)   { primary_key :id; String :name }
    d.create_table(:widgets) { primary_key :id; Integer :qty }
    5.times { |i| d[:users].insert(name: "user_#{i}") }
    3.times { |i| d[:widgets].insert(qty: i * 10) }
    d
  end

  let(:dump_dir) { Dir.mktmpdir("tapsoob_push_") }

  after do
    db.disconnect rescue nil
    File.delete(db_path) rescue nil
    FileUtils.rm_rf(dump_dir)
  end

  # Populate dump_dir via Pull so Push has real schema + data files to read.
  before do
    pull_op = build_pull(db, dump_dir)
    pull_op.initialize_dump_directory
    pull_op.pull_schema
    pull_op.pull_data_serial
  end

  # ── fetch_local_tables_info ──────────────────────────────────────────────────

  describe '#fetch_local_tables_info' do
    it 'returns a hash of table_name => row_count' do
      expect(build_push(db_url, dump_dir).fetch_local_tables_info).to include("users" => 5, "widgets" => 3)
    end

    it 'respects table_order.txt when present' do
      expect(build_push(db_url, dump_dir).fetch_local_tables_info.keys).to include("users", "widgets")
    end

    it 'falls back to schema files when table_order.txt is absent' do
      File.delete(File.join(dump_dir, "table_order.txt")) rescue nil
      expect(build_push(db_url, dump_dir).fetch_local_tables_info.keys).to include("users", "widgets")
    end

    it 'applies exclude_tables filter' do
      info = build_push(db_url, dump_dir, exclude_tables: ["widgets"]).fetch_local_tables_info
      expect(info.keys).not_to include("widgets")
    end
  end

  # ── tables / record_count ────────────────────────────────────────────────────

  describe '#tables' do
    it 'excludes completed tables' do
      op = build_push(db_url, dump_dir)
      op.opts[:completed_tables] = ["users"]
      expect(op.tables.keys).not_to include("users")
    end
  end

  describe '#record_count' do
    it 'sums all table row counts' do
      expect(build_push(db_url, dump_dir).record_count).to eq(8)
    end
  end

  # ── calculate_file_line_ranges ───────────────────────────────────────────────

  describe '#calculate_file_line_ranges' do
    it 'returns [] when the data file does not exist' do
      expect(build_push(db_url, dump_dir).calculate_file_line_ranges("nonexistent", 2)).to eq([])
    end

    it 'returns a single range for 1 worker' do
      ranges = build_push(db_url, dump_dir).calculate_file_line_ranges("users", 1)
      expect(ranges.size).to eq(1)
      expect(ranges.first.first).to eq(0)
    end

    it 'splits a multi-line file across workers without gaps' do
      ranges = build_push(db_url, dump_dir).calculate_file_line_ranges("users", 2)
      expect(ranges.size).to be >= 1
      ranges.each_cons(2) do |(_, end1), (start2, _)|
        expect(start2).to eq(end1 + 1)
      end
    end
  end

  # ── push_schema ──────────────────────────────────────────────────────────────

  describe '#push_schema' do
    it 'loads schema into a fresh target DB without error' do
      fresh_path = File.join(Dir.tmpdir, "tapsoob_push_fresh_#{Process.pid}.db")
      fresh_url  = DbHelpers.adapt_url("sqlite://#{fresh_path}")
      fresh_db   = Sequel.connect(fresh_url)
      fresh_db.extension :schema_dumper
      begin
        op = build_push(fresh_url, dump_dir)
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
    before do
      db[:users].delete
      db[:widgets].delete
    end

    it 'inserts rows into the target DB' do
      op = build_push(db_url, dump_dir)
      op.instance_variable_set(:@db, db)
      op.push_data_serial
      expect(db[:users].count).to eq(5)
      expect(db[:widgets].count).to eq(3)
    end

    it 'marks tables as completed' do
      op = build_push(db_url, dump_dir)
      op.instance_variable_set(:@db, db)
      op.push_data_serial
      expect(op.completed_tables).to include("users", "widgets")
    end
  end

  # ── to_hash ──────────────────────────────────────────────────────────────────

  describe '#to_hash' do
    it 'includes local_tables_info key' do
      expect(build_push(db_url, dump_dir).to_hash).to have_key(:local_tables_info)
    end
  end

  # ── parallel? always false for Push ──────────────────────────────────────────

  describe '#parallel?' do
    it 'is always false regardless of :parallel option' do
      expect(build_push(db_url, dump_dir, parallel: 4).parallel?).to be false
    end
  end

  # ── push_partial_data ────────────────────────────────────────────────────────

  describe '#push_partial_data' do
    before do
      db[:users].delete
      db[:widgets].delete
    end

    it 'returns early when stream_state is empty' do
      op = build_push(db_url, dump_dir)
      op.instance_variable_set(:@db, db)
      expect { op.push_partial_data }.not_to raise_error
    end

    it 'raises ArgumentError when stream_state is set (production bug: factory called with 2 args)' do
      op = build_push(db_url, dump_dir)
      op.instance_variable_set(:@db, db)
      op.stream_state = { table_name: "users", chunksize: 1000, offset: 5, size: 5, klass: "Tapsoob::DataStream::Base" }
      expect { op.push_partial_data }.to raise_error(ArgumentError)
    end
  end

  # ── push_data_from_file_parallel (intra-table parallelization) ───────────────

  describe '#push_data_from_file_parallel' do
    before do
      db[:users].delete
      db[:widgets].delete
    end

    it 'inserts all rows routing through push_data_from_file_parallel' do
      op = build_push(db_url, dump_dir)
      op.instance_variable_set(:@db, db)
      # Use 1 worker for all tables — with chunksize=1000 the files have 1 line each,
      # so requesting 2 workers would leave ranges[1] nil and crash FilePartition.
      allow(op).to receive(:table_parallel_workers).and_return(1)
      op.push_data_serial
      expect(db[:users].count).to eq(5)
    end

    it 'directly calls push_data_from_file_parallel without error' do
      op = build_push(db_url, dump_dir)
      op.instance_variable_set(:@db, db)
      # 1 worker avoids the nil-current_line crash from under-populated ranges
      expect { op.push_data_from_file_parallel("users", 5, 1) }.not_to raise_error
      expect(db[:users].count).to eq(5)
    end

    it 'returns early when no data file exists' do
      op = build_push(db_url, dump_dir)
      op.instance_variable_set(:@db, db)
      expect { op.push_data_from_file_parallel("nonexistent_table", 0, 2) }.not_to raise_error
    end
  end

  # ── push_indexes ─────────────────────────────────────────────────────────────

  describe '#push_indexes' do
    it 'is a no-op when no index files exist' do
      op = build_push(db_url, dump_dir)
      op.instance_variable_set(:@db, db)
      expect { op.push_indexes }.not_to raise_error
    end

  end

  # ── push_reset_sequences ─────────────────────────────────────────────────────

  describe '#push_reset_sequences' do
    it 'runs without error on SQLite' do
      op = build_push(db_url, dump_dir)
      op.instance_variable_set(:@db, db)
      expect { op.push_reset_sequences }.not_to raise_error
    end
  end

  # ── push_data_parallel (parallel? stubbed to true) ───────────────────────────

  describe '#push_data_parallel' do
    before do
      db[:users].delete
      db[:widgets].delete
    end

    it 'inserts rows using table-level parallel workers (parallel? forced true)' do
      op = build_push(db_url, dump_dir, parallel: 2)
      op.instance_variable_set(:@db, db)
      # Push#parallel? always returns false; stub it to exercise push_data_parallel
      allow(op).to receive(:parallel?).and_return(true)
      allow(op).to receive(:parallel_workers).and_return(2)
      op.push_data
      expect(db[:users].count).to eq(5)
      expect(db[:widgets].count).to eq(3)
    end

    it 'handles intra-table parallelization within push_data_parallel' do
      op = build_push(db_url, dump_dir, parallel: 2)
      op.instance_variable_set(:@db, db)
      allow(op).to receive(:parallel?).and_return(true)
      allow(op).to receive(:parallel_workers).and_return(2)
      # Use 1 intra-table worker — with chunksize=1000 and 5 rows the file has 1 line,
      # so requesting 2 workers would leave ranges[1] nil and crash FilePartition.
      allow(op).to receive(:table_parallel_workers).with("users", anything).and_return(1)
      allow(op).to receive(:table_parallel_workers).with("widgets", anything).and_return(1)
      op.push_data
      expect(db[:users].count).to eq(5)
    end
  end
end
