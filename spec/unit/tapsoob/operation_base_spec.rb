require 'spec_helper'
require 'tapsoob/operation/base'
require 'tapsoob/operation/pull'
require 'tapsoob/operation/push'

RSpec.describe Tapsoob::Operation::Base do
  let(:url)     { sqlite_memory_url }
  let(:dir)     { Dir.mktmpdir }
  let(:base_opts) { { data: true, schema: true, progress: false, default_chunksize: 1000 } }

  after { FileUtils.rm_rf(dir) }

  subject(:op) { described_class.new(url, dir, base_opts) }

  # ── table_filter / exclude_tables ────────────────────────────────────────────

  describe '#apply_table_filter' do
    context 'when no filter is set' do
      it 'returns all tables unchanged' do
        tables = ['users', 'orders', 'products']
        expect(op.apply_table_filter(tables)).to eq(tables)
      end
    end

    context 'with :tables filter' do
      subject(:op) { described_class.new(url, dir, base_opts.merge(tables: ['users'])) }

      it 'keeps only whitelisted tables' do
        tables = ['users', 'orders']
        expect(op.apply_table_filter(tables)).to eq(['users'])
      end
    end

    context 'with :exclude_tables' do
      subject(:op) { described_class.new(url, dir, base_opts.merge(exclude_tables: ['orders'])) }

      it 'excludes the specified tables' do
        tables = ['users', 'orders', 'products']
        expect(op.apply_table_filter(tables)).to eq(['users', 'products'])
      end
    end

    context 'with a Hash argument' do
      subject(:op) { described_class.new(url, dir, base_opts.merge(tables: ['users'])) }

      it 'filters the hash by whitelisted keys' do
        tables = { 'users' => 10, 'orders' => 5 }
        expect(op.apply_table_filter(tables)).to eq({ 'users' => 10 })
      end
    end
  end

  # ── parallelism ──────────────────────────────────────────────────────────────

  describe '#parallel_workers' do
    it 'defaults to 1 when no --parallel option given' do
      expect(op.parallel_workers).to eq(1)
    end

    it 'respects the :parallel option' do
      o = described_class.new(url, dir, base_opts.merge(parallel: 4))
      expect(o.parallel_workers).to eq(4)
    end

    it 'is at least 1 for invalid values' do
      o = described_class.new(url, dir, base_opts.merge(parallel: 0))
      expect(o.parallel_workers).to be >= 1
    end
  end

  describe '#table_parallel_workers' do
    it 'returns 1 when dump_path is nil (piped mode)' do
      o = described_class.new(url, nil, base_opts)
      expect(o.table_parallel_workers(:big_table, 500_000)).to eq(1)
    end

    it 'returns 1 for tables under 100K rows' do
      expect(op.table_parallel_workers(:small_table, 99_999)).to eq(1)
    end

    it 'returns >= 2 for tables at the 100K threshold' do
      expect(op.table_parallel_workers(:big_table, 100_000)).to be >= 2
    end

    it 'returns 1 when --no-split is set' do
      o = described_class.new(url, dir, base_opts.merge(no_split: true))
      expect(o.table_parallel_workers(:big_table, 500_000)).to eq(1)
    end
  end

  # ── completed_tables (thread safety) ─────────────────────────────────────────

  describe '#add_completed_table' do
    it 'adds the table name as a string' do
      op.add_completed_table(:users)
      expect(op.completed_tables).to include('users')
    end

    it 'is thread-safe under concurrent adds' do
      threads = 20.times.map { |i| Thread.new { op.add_completed_table("table_#{i}") } }
      threads.each(&:join)
      expect(op.completed_tables.uniq.size).to eq(20)
    end
  end

  # ── factory ──────────────────────────────────────────────────────────────────

  describe '.factory' do
    it 'returns a Pull operation for :pull' do
      result = described_class.factory(:pull, url, dir, base_opts)
      expect(result).to be_a(Tapsoob::Operation::Pull)
    end

    it 'returns a Push operation for :push' do
      result = described_class.factory(:push, url, dir, base_opts)
      expect(result).to be_a(Tapsoob::Operation::Push)
    end

    it 'raises on unknown type' do
      expect {
        described_class.factory(:unknown, url, dir, base_opts)
      }.to raise_error(RuntimeError, /Unknown Operation Type/)
    end
  end

  # ── session persistence ───────────────────────────────────────────────────────

  describe '#to_hash' do
    it 'includes klass, database_url, stream_state, completed_tables' do
      h = op.to_hash
      expect(h).to include(:klass, :database_url, :stream_state, :completed_tables)
    end
  end
end
