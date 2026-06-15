require 'spec_helper'
require 'tapsoob/operation/base'
require 'tapsoob/operation/pull'
require 'tapsoob/operation/push'

RSpec.describe Tapsoob::Operation::Base do
  let(:db)       { seeded_sqlite_db }
  let(:dump_dir) { Dir.mktmpdir("tapsoob_base_") }

  after do
    db.disconnect
    FileUtils.rm_rf(dump_dir)
  end

  # ── format_number ────────────────────────────────────────────────────────────

  describe '#format_number' do
    it 'formats numbers with commas' do
      op = build_base(db, dump_dir)
      expect(op.format_number(1_000_000)).to eq("1,000,000")
      expect(op.format_number(1234)).to eq("1,234")
      expect(op.format_number(999)).to eq("999")
    end
  end

  # ── resuming? ────────────────────────────────────────────────────────────────

  describe '#resuming?' do
    it 'returns false by default' do
      expect(build_base(db, dump_dir).resuming?).to be false
    end

    it 'returns true when :resume is set' do
      expect(build_base(db, dump_dir, resume: true).resuming?).to be true
    end
  end

  # ── parallel? / parallel_workers ─────────────────────────────────────────────

  describe '#parallel?' do
    it 'returns false when parallel is 1' do
      expect(build_base(db, dump_dir, parallel: 1).parallel?).to be false
    end

    it 'returns true when parallel > 1' do
      expect(build_base(db, dump_dir, parallel: 2).parallel?).to be true
    end
  end

  describe '#parallel_workers' do
    it 'defaults to 1' do
      expect(build_base(db, dump_dir).parallel_workers).to eq(1)
    end

    it 'returns the requested count' do
      expect(build_base(db, dump_dir, parallel: 4).parallel_workers).to eq(4)
    end
  end

  # ── table_parallel_workers ───────────────────────────────────────────────────

  describe '#table_parallel_workers' do
    it 'returns 1 when no_split is set' do
      expect(build_base(db, dump_dir, no_split: true).table_parallel_workers(:users, 5_000_000)).to eq(1)
    end

    it 'returns 1 when dump_path is nil' do
      op = Tapsoob::Operation::Pull.new(sqlite_memory_url, nil, { default_chunksize: 1000 })
      expect(op.table_parallel_workers(:users, 5_000_000)).to eq(1)
    end

    it 'returns 1 when row_count is below threshold' do
      expect(build_base(db, dump_dir, no_split: false).table_parallel_workers(:users, 50_000)).to eq(1)
    end

    it 'returns >= 2 for a very large table' do
      expect(build_base(db, dump_dir, no_split: false).table_parallel_workers(:users, 5_000_000)).to be >= 2
    end

    it 'returns >= 2 for a 1M+ row table' do
      expect(build_base(db, dump_dir, no_split: false).table_parallel_workers(:users, 1_000_000)).to be >= 2
    end

    it 'returns >= 2 for a 500K+ row table' do
      expect(build_base(db, dump_dir, no_split: false).table_parallel_workers(:users, 500_000)).to be >= 2
    end

    it 'returns 2 for a table just over the 100K threshold' do
      expect(build_base(db, dump_dir, no_split: false).table_parallel_workers(:users, 150_000)).to eq(2)
    end
  end

  # ── stream_state ─────────────────────────────────────────────────────────────

  describe '#stream_state / #stream_state=' do
    it 'defaults to empty hash' do
      expect(build_base(db, dump_dir).stream_state).to eq({})
    end

    it 'stores and retrieves state' do
      op = build_base(db, dump_dir)
      op.stream_state = { table_name: :users }
      expect(op.stream_state).to eq({ table_name: :users })
    end
  end

  # ── add_completed_table ──────────────────────────────────────────────────────

  describe '#add_completed_table' do
    it 'appends to completed_tables thread-safely' do
      op = build_base(db, dump_dir)
      op.add_completed_table(:users)
      op.add_completed_table(:widgets)
      expect(op.completed_tables).to include("users", "widgets")
    end
  end

  # ── max_intra_table_workers ──────────────────────────────────────────────────

  describe '#max_intra_table_workers' do
    it 'returns at least 2' do
      expect(build_base(db, dump_dir).max_intra_table_workers).to be >= 2
    end
  end

  # ── catch_errors ─────────────────────────────────────────────────────────────

  describe '#catch_errors' do
    it 'yields and returns the block result' do
      expect(build_base(db, dump_dir).send(:catch_errors) { 42 }).to eq(42)
    end

    it 're-raises exceptions' do
      op = build_base(db, dump_dir)
      expect { op.send(:catch_errors) { raise ArgumentError, "boom" } }.to raise_error(ArgumentError, "boom")
    end
  end

  # ── apply_table_filter (array form) ──────────────────────────────────────────

  describe '#apply_table_filter' do
    it 'filters an array by table_filter' do
      op = build_base(db, dump_dir, tables: ["users"])
      expect(op.apply_table_filter(["users", "widgets"])).to eq(["users"])
    end

    it 'excludes tables from an array' do
      op = build_base(db, dump_dir, exclude_tables: ["widgets"])
      expect(op.apply_table_filter(["users", "widgets"])).to eq(["users"])
    end
  end

  # ── Base.factory ─────────────────────────────────────────────────────────────

  describe '.factory' do
    it 'returns a Pull instance for :pull type' do
      expect(described_class.factory(:pull, sqlite_memory_url, dump_dir, { default_chunksize: 1000 })).to be_a(Tapsoob::Operation::Pull)
    end

    it 'returns a Push instance for :push type' do
      expect(described_class.factory(:push, sqlite_memory_url, dump_dir, { default_chunksize: 1000 })).to be_a(Tapsoob::Operation::Push)
    end

    it 'raises for unknown type' do
      expect { described_class.factory(:unknown, sqlite_memory_url, dump_dir, {}) }
        .to raise_error(RuntimeError, /Unknown Operation Type/)
    end

    it 'returns a resume instance when opts[:resume] is true' do
      op = build_pull(db, dump_dir)
      op.initialize_dump_directory
      op.pull_schema

      # Pull#to_hash calls remote_tables_info which requires an active pull run;
      # use the base to_hash binding to get just the serializable fields.
      hash = Tapsoob::Operation::Base.instance_method(:to_hash).bind(op).call
      resumed = described_class.factory(:pull, sqlite_memory_url, dump_dir,
        hash.merge(resume: true, klass: "Tapsoob::Operation::Pull", default_chunksize: 1000))
      expect(resumed).to be_a(Tapsoob::Operation::Pull)
    end
  end

  # ── exiting? / setup_signal_trap ─────────────────────────────────────────────

  describe '#exiting?' do
    it 'returns false initially' do
      expect(build_base(db, dump_dir).exiting?).to be false
    end
  end

  describe '#setup_signal_trap' do
    it 'registers signal handlers without error' do
      op = build_base(db, dump_dir)
      expect { op.setup_signal_trap }.not_to raise_error
    end
  end

  # ── can_use_pk_partitioning? ─────────────────────────────────────────────────

  describe '#can_use_pk_partitioning?' do
    it 'returns true for a table with a single integer PK' do
      op = build_base(db, dump_dir)
      expect(op.can_use_pk_partitioning?(:users)).to be true
    end
  end

  # ── db / default_chunksize ───────────────────────────────────────────────────

  describe '#default_chunksize' do
    it 'returns the value from opts' do
      expect(build_base(db, dump_dir, default_chunksize: 500).default_chunksize).to eq(500)
    end
  end

  describe '#table_filter / #exclude_tables' do
    it 'returns empty arrays by default' do
      op = build_base(db, dump_dir)
      expect(op.table_filter).to eq([])
      expect(op.exclude_tables).to eq([])
    end
  end
end
