require 'spec_helper'
require 'tapsoob/operation/base'
require 'tapsoob/operation/pull'
require 'tapsoob/operation/push'
require 'tmpdir'
require 'fileutils'

RSpec.describe Tapsoob::Operation::Base do
  let(:dump_dir) { Dir.mktmpdir("tapsoob_base_") }
  after { FileUtils.rm_rf(dump_dir) }

  def build_base(extra_opts = {})
    opts = { default_chunksize: 1000, no_split: true }.merge(extra_opts)
    # Use Pull as a concrete subclass (Base is abstract)
    op = Tapsoob::Operation::Pull.new(sqlite_memory_url, dump_dir, opts)
    op.instance_variable_set(:@db, connect_sqlite)
    op
  end

  # ── format_number ────────────────────────────────────────────────────────────

  describe '#format_number' do
    it 'formats numbers with commas' do
      op = build_base
      expect(op.format_number(1_000_000)).to eq("1,000,000")
      expect(op.format_number(1234)).to eq("1,234")
      expect(op.format_number(999)).to eq("999")
    end
  end

  # ── resuming? ────────────────────────────────────────────────────────────────

  describe '#resuming?' do
    it 'returns false by default' do
      expect(build_base.resuming?).to be false
    end

    it 'returns true when :resume is set' do
      expect(build_base(resume: true).resuming?).to be true
    end
  end

  # ── parallel? / parallel_workers ─────────────────────────────────────────────

  describe '#parallel?' do
    it 'returns false when parallel is 1' do
      expect(build_base(parallel: 1).parallel?).to be false
    end

    it 'returns true when parallel > 1' do
      expect(build_base(parallel: 2).parallel?).to be true
    end
  end

  describe '#parallel_workers' do
    it 'defaults to 1' do
      expect(build_base.parallel_workers).to eq(1)
    end

    it 'returns the requested count' do
      expect(build_base(parallel: 4).parallel_workers).to eq(4)
    end
  end

  # ── table_parallel_workers ───────────────────────────────────────────────────

  describe '#table_parallel_workers' do
    it 'returns 1 when no_split is set' do
      op = build_base(no_split: true)
      expect(op.table_parallel_workers(:users, 5_000_000)).to eq(1)
    end

    it 'returns 1 when dump_path is nil' do
      op = Tapsoob::Operation::Pull.new(sqlite_memory_url, nil, { default_chunksize: 1000 })
      expect(op.table_parallel_workers(:users, 5_000_000)).to eq(1)
    end

    it 'returns 1 when row_count is below threshold' do
      op = build_base
      expect(op.table_parallel_workers(:users, 50_000)).to eq(1)
    end

    it 'returns >= 2 for a very large table' do
      op = build_base
      expect(op.table_parallel_workers(:users, 5_000_000)).to be >= 2
    end

    it 'returns >= 2 for a 1M+ row table' do
      op = build_base
      expect(op.table_parallel_workers(:users, 1_000_000)).to be >= 2
    end

    it 'returns >= 2 for a 500K+ row table' do
      op = build_base
      expect(op.table_parallel_workers(:users, 500_000)).to be >= 2
    end

    it 'returns 2 for a table just over the 100K threshold' do
      op = build_base
      expect(op.table_parallel_workers(:users, 150_000)).to eq(2)
    end
  end

  # ── stream_state ─────────────────────────────────────────────────────────────

  describe '#stream_state / #stream_state=' do
    it 'defaults to empty hash' do
      expect(build_base.stream_state).to eq({})
    end

    it 'stores and retrieves state' do
      op = build_base
      op.stream_state = { table_name: :users }
      expect(op.stream_state).to eq({ table_name: :users })
    end
  end

  # ── add_completed_table ──────────────────────────────────────────────────────

  describe '#add_completed_table' do
    it 'appends to completed_tables thread-safely' do
      op = build_base
      op.add_completed_table(:users)
      op.add_completed_table(:widgets)
      expect(op.completed_tables).to include("users", "widgets")
    end
  end

  # ── max_intra_table_workers ──────────────────────────────────────────────────

  describe '#max_intra_table_workers' do
    it 'returns at least 2' do
      expect(build_base.max_intra_table_workers).to be >= 2
    end
  end

  # ── catch_errors ─────────────────────────────────────────────────────────────

  describe '#catch_errors' do
    it 'yields and returns the block result' do
      op = build_base
      result = op.send(:catch_errors) { 42 }
      expect(result).to eq(42)
    end

    it 're-raises exceptions' do
      op = build_base
      expect { op.send(:catch_errors) { raise ArgumentError, "boom" } }.to raise_error(ArgumentError, "boom")
    end
  end

  # ── apply_table_filter (array form) ──────────────────────────────────────────

  describe '#apply_table_filter' do
    it 'filters an array by table_filter' do
      op = build_base(tables: ["users"])
      expect(op.apply_table_filter(["users", "widgets"])).to eq(["users"])
    end

    it 'excludes tables from an array' do
      op = build_base(exclude_tables: ["widgets"])
      expect(op.apply_table_filter(["users", "widgets"])).to eq(["users"])
    end
  end

  # ── Base.factory ─────────────────────────────────────────────────────────────

  describe '.factory' do
    it 'returns a Pull instance for :pull type' do
      op = described_class.factory(:pull, sqlite_memory_url, dump_dir, { default_chunksize: 1000 })
      expect(op).to be_a(Tapsoob::Operation::Pull)
    end

    it 'returns a Push instance for :push type' do
      op = described_class.factory(:push, sqlite_memory_url, dump_dir, { default_chunksize: 1000 })
      expect(op).to be_a(Tapsoob::Operation::Push)
    end

    it 'raises for unknown type' do
      expect { described_class.factory(:unknown, sqlite_memory_url, dump_dir, {}) }
        .to raise_error(RuntimeError, /Unknown Operation Type/)
    end
  end
end
