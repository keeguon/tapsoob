require 'spec_helper'
require 'tapsoob/data_stream'

# Shared low-level stream tests run against an in-process SQLite connection.
# They exercise the fetch/encode/decode cycle, completion logic, and the
# factory method – all without any filesystem dump.

RSpec.describe Tapsoob::DataStream do
  # ── shared DB setup ──────────────────────────────────────────────────────────

  let(:db) do
    d = connect_sqlite
    d.extension :schema_dumper
    d.create_table(:stream_test) do
      primary_key :id
      String  :label, size: 50
      Integer :value
    end
    # Insert 50 rows
    50.times { |i| d[:stream_test].insert(label: "row_#{i}", value: i) }
    d
  end

  let(:db_nopk) do
    d = connect_sqlite
    d.extension :schema_dumper
    d.create_table(:nopk_test) do
      String  :key, size: 50
      Integer :val
    end
    20.times { |i| d[:nopk_test].insert(key: "k#{i}", val: i) }
    d
  end

  after { db.disconnect; db_nopk.disconnect }

  # ── Base ─────────────────────────────────────────────────────────────────────

  describe Tapsoob::DataStream::Base do
    subject(:stream) do
      described_class.new(db, { table_name: :stream_test, chunksize: 10 })
    end

    describe '#fetch' do
      it 'returns [encoded_data, row_count, elapsed]' do
        encoded, count, elapsed = stream.fetch
        expect(encoded).to be_a(String)
        expect(count).to eq(10)
        expect(elapsed).to be_a(Float)
      end

      it 'advances offset on each call' do
        stream.fetch
        expect(stream.state[:offset]).to eq(10)
        stream.fetch
        expect(stream.state[:offset]).to eq(20)
      end
    end

    describe '#complete?' do
      it 'is false before all rows are fetched' do
        stream.fetch
        expect(stream.complete?).to be false
      end

      it 'is true after all rows are fetched' do
        5.times { stream.fetch }
        expect(stream.complete?).to be true
      end
    end

    describe '#fetch_data_from_database' do
      it 'yields a hash with :table_name, :header, :data, :types' do
        encoded, _, _ = stream.fetch
        data_params = {
          state:        stream.to_hash,
          checksum:     Tapsoob::Utils.checksum(encoded).to_s,
          encoded_data: encoded
        }
        yielded = nil
        stream.fetch_data_from_database(data_params) { |rows| yielded = rows }
        expect(yielded).to include(:table_name, :header, :data)
      end
    end

    describe '#parse_encoded_data' do
      it 'raises CorruptedData on checksum mismatch' do
        encoded, _, _ = stream.fetch
        expect {
          stream.parse_encoded_data(encoded, '0')
        }.to raise_error(Tapsoob::CorruptedData)
      end
    end

    describe '.factory' do
      it 'returns Keyed stream for table with integer PK' do
        result = described_class.factory(db, { table_name: :stream_test, chunksize: 10 }, {})
        expect(result).to be_a(Tapsoob::DataStream::Keyed)
      end

      it 'returns Base stream for table without integer PK' do
        result = described_class.factory(db_nopk, { table_name: :nopk_test, chunksize: 10 }, {})
        expect(result).to be_a(Tapsoob::DataStream::Base)
      end
    end
  end

  # ── Keyed ────────────────────────────────────────────────────────────────────

  describe Tapsoob::DataStream::Keyed do
    subject(:stream) do
      described_class.new(db, { table_name: :stream_test, chunksize: 10 })
    end

    it 'fetches all 50 rows without duplicates' do
      all_ids = []
      until stream.complete?
        encoded, _, _ = stream.fetch
        data_params = {
          state:        stream.to_hash,
          checksum:     Tapsoob::Utils.checksum(encoded).to_s,
          encoded_data: encoded
        }
        stream.fetch_data_from_database(data_params) do |rows|
          id_idx = rows[:header].index(:id)
          all_ids.concat(rows[:data].map { |r| r[id_idx] })
        end
      end
      expect(all_ids.uniq.size).to eq(50)
      expect(all_ids.size).to eq(50)
    end

    describe '.calculate_pk_ranges' do
      it 'returns the right number of ranges' do
        ranges = described_class.calculate_pk_ranges(db, :stream_test, 4)
        expect(ranges.size).to eq(4)
      end

      it 'covers the full PK range' do
        min = db[:stream_test].min(:id)
        max = db[:stream_test].max(:id)
        ranges = described_class.calculate_pk_ranges(db, :stream_test, 4)
        expect(ranges.first.first).to eq(min)
        expect(ranges.last.last).to eq(max)
      end
    end
  end

  # ── KeyedPartition ───────────────────────────────────────────────────────────

  describe Tapsoob::DataStream::KeyedPartition do
    it 'fetches only rows within its assigned PK range' do
      min = db[:stream_test].min(:id)
      max = db[:stream_test].max(:id)
      mid = (min + max) / 2

      stream = described_class.new(db, {
        table_name:      :stream_test,
        chunksize:       100,
        partition_range: [min, mid]
      })

      all_ids = []
      until stream.complete?
        encoded, count, _ = stream.fetch
        break if count == 0
        data_params = {
          state:        stream.to_hash,
          checksum:     Tapsoob::Utils.checksum(encoded).to_s,
          encoded_data: encoded
        }
        stream.fetch_data_from_database(data_params) do |rows|
          id_idx = rows[:header].index(:id)
          all_ids.concat(rows[:data].map { |r| r[id_idx] })
        end
      end

      expect(all_ids).to all(be_between(min, mid))
    end
  end

  # ── Interleaved ──────────────────────────────────────────────────────────────

  describe Tapsoob::DataStream::Interleaved do
    it 'two workers together cover all rows without overlap' do
      worker0 = described_class.new(db, {
        table_name: :stream_test, chunksize: 10, worker_id: 0, num_workers: 2
      })
      worker1 = described_class.new(db, {
        table_name: :stream_test, chunksize: 10, worker_id: 1, num_workers: 2
      })

      def drain(stream, db)
        ids = []
        until stream.complete?
          encoded, count, _ = stream.fetch
          break if count == 0
          params = {
            state:        stream.to_hash,
            checksum:     Tapsoob::Utils.checksum(encoded).to_s,
            encoded_data: encoded
          }
          stream.fetch_data_from_database(params) do |rows|
            id_idx = rows[:header].index(:id)
            ids.concat(rows[:data].map { |r| r[id_idx] })
          end
        end
        ids
      end

      ids0 = drain(worker0, db)
      ids1 = drain(worker1, db)

      expect((ids0 + ids1).sort).to eq((ids0 + ids1).uniq.sort)
      expect((ids0 + ids1).size).to eq(50)
    end
  end
end
