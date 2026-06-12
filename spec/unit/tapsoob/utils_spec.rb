require 'spec_helper'
require 'tapsoob/utils'

RSpec.describe Tapsoob::Utils do
  # ── checksum / valid_data ────────────────────────────────────────────────────

  describe '.checksum' do
    it 'returns a Zlib CRC32 integer' do
      expect(described_class.checksum('hello')).to be_a(Integer)
    end

    it 'is deterministic for the same input' do
      expect(described_class.checksum('test')).to eq(described_class.checksum('test'))
    end

    it 'differs for different inputs' do
      expect(described_class.checksum('a')).not_to eq(described_class.checksum('b'))
    end
  end

  describe '.valid_data?' do
    let(:data) { 'some payload' }
    let(:crc)  { described_class.checksum(data) }

    it 'returns true when checksum matches' do
      expect(described_class.valid_data?(data, crc)).to be true
    end

    it 'returns false when checksum does not match' do
      expect(described_class.valid_data?(data, crc + 1)).to be false
    end

    it 'accepts crc as a string (mirrors production code path)' do
      expect(described_class.valid_data?(data, crc.to_s)).to be true
    end
  end

  # ── base64 round-trip ────────────────────────────────────────────────────────

  describe '.base64encode / .base64decode' do
    it 'round-trips plain text' do
      text = 'Hello, Tapsoob!'
      expect(described_class.base64decode(described_class.base64encode(text))).to eq(text)
    end

    it 'round-trips binary data faithfully' do
      binary = Random.bytes(512)
      decoded = described_class.base64decode(described_class.base64encode(binary))
      expect(decoded.bytes).to eq(binary.bytes)
    end

    it 'round-trips empty string' do
      expect(described_class.base64decode(described_class.base64encode(''))).to eq('')
    end
  end

  # ── format_data ──────────────────────────────────────────────────────────────

  describe '.format_data' do
    let(:db) do
      d = connect_sqlite
      d.create_table(:fmt_test) do
        primary_key :id
        String  :name, size: 50
        Integer :score
        File    :payload
      end
      d
    end

    # Evaluate schema in the same let-chain so it always uses the same db instance.
    let(:schema) { db.schema(:fmt_test) }

    after { db.disconnect }

    it 'returns {} for empty data' do
      expect(described_class.format_data(db, [], schema: schema, table: :fmt_test)).to eq({})
    end

    it 'builds header, data, types for normal rows' do
      data = [{ id: 1, name: 'Alice', score: 42, payload: nil }]
      result = described_class.format_data(db, data, schema: schema, table: :fmt_test)
      expect(result[:header]).to include(:id, :name, :score, :payload)
      expect(result[:data].first).to include(1, 'Alice', 42)
      expect(result[:types]).to include('integer', 'string')
    end

    it 'marks File columns as "blob" type' do
      data = [{ id: 1, name: 'x', score: 0, payload: Sequel::SQL::Blob.new('bin') }]
      result = described_class.format_data(db, data, schema: schema, table: :fmt_test)
      payload_idx = result[:header].index(:payload)
      expect(result[:types][payload_idx]).to eq('blob')
    end

    it 'converts Time objects to ISO-8601 strings' do
      db2 = connect_sqlite
      db2.extension :schema_dumper
      db2.create_table(:ts_test) { primary_key :id; DateTime :ts }
      t = Time.utc(2024, 6, 15, 12, 0, 0)
      data = [{ id: 1, ts: t }]
      result = described_class.format_data(db2, data,
        schema: db2.schema(:ts_test), table: :ts_test)
      ts_idx = result[:header].index(:ts)
      expect(result[:data].first[ts_idx]).to eq('2024-06-15 12:00:00')
      db2.disconnect
    end
  end

  # ── encode_blobs ─────────────────────────────────────────────────────────────

  describe '.encode_blobs' do
    it 'base64-encodes Sequel::SQL::Blob values' do
      blob = Sequel::SQL::Blob.new('binary data')
      row  = { payload: blob }
      described_class.encode_blobs(row, [:payload])
      expect(row[:payload]).to eq(described_class.base64encode('binary data'))
    end

    it 'leaves non-blob string columns unchanged' do
      row = { name: 'Alice' }
      described_class.encode_blobs(row, [])
      expect(row[:name]).to eq('Alice')
    end

    it 'encodes Blob values not in the columns list (auto-detect)' do
      blob = Sequel::SQL::Blob.new('surprise')
      row  = { payload: blob }
      described_class.encode_blobs(row, [])
      expect(row[:payload]).to eq(described_class.base64encode('surprise'))
    end

    it 'skips nil blob values' do
      row = { payload: nil }
      described_class.encode_blobs(row, [:payload])
      expect(row[:payload]).to be_nil
    end
  end

  # ── calculate_chunksize ──────────────────────────────────────────────────────

  describe '.calculate_chunksize' do
    it 'returns an Integer' do
      result = described_class.calculate_chunksize(1000) { |_c| 0.5 }
      expect(result).to be_a(Integer)
    end

    it 'increases chunksize when block returns fast time (< 0.8s)' do
      result = described_class.calculate_chunksize(1000) { |_c| 0.1 }
      expect(result).to be > 1000
    end

    it 'decreases chunksize when block returns slow time (> 3s)' do
      # time_in_db must be near-zero so the real wall-clock diff drives the decision.
      # We fake it by setting start/end times directly on the Chunksize object.
      result = described_class.calculate_chunksize(900) do |c|
        c.start_time = Time.now - 4.5  # pretend we started 4.5s ago
        0.0                            # time_in_db ≈ 0 → diff = 4.5 > 3.0 → halve
      end
      expect(result).to be < 900
    end

    it 'retries up to 2 times on EPIPE and then re-raises' do
      calls = 0
      expect {
        described_class.calculate_chunksize(1000) do |c|
          calls += 1
          raise Errno::EPIPE
        end
      }.to raise_error(Errno::EPIPE)
      expect(calls).to eq(3) # initial + 2 retries
    end
  end

  # ── primary_key / single_integer_primary_key / order_by ─────────────────────

  describe 'primary key helpers' do
    let(:db) do
      d = connect_sqlite
      d.create_table(:pk_test)  { primary_key :id; String :name }
      d.create_table(:cpk_test) { String :a; String :b; primary_key [:a, :b] }
      d.create_table(:nopk)     { String :x; String :y }
      d
    end

    after { db.disconnect }

    describe '.primary_key' do
      it 'returns [:id] for a single-column integer PK' do
        expect(described_class.primary_key(db, :pk_test)).to eq([:id])
      end

      it 'returns both columns for a composite PK' do
        expect(described_class.primary_key(db, :cpk_test)).to match_array([:a, :b])
      end
    end

    describe '.single_integer_primary_key' do
      it 'is true for a single integer PK' do
        expect(described_class.single_integer_primary_key(db, :pk_test)).to be true
      end

      it 'is false for composite PK' do
        expect(described_class.single_integer_primary_key(db, :cpk_test)).to be false
      end

      it 'is false for a table with no PK' do
        expect(described_class.single_integer_primary_key(db, :nopk)).to be false
      end
    end

    describe '.order_by' do
      it 'returns the PK column as an array for keyed tables' do
        expect(described_class.order_by(db, :pk_test)).to eq([:id])
      end

      it 'returns all columns for tables without a single integer PK' do
        # Sequel needs at least one query against the table for #columns to be populated.
        db[:nopk].first
        result = described_class.order_by(db, :nopk)
        expect(result).to match_array([:x, :y])
      end
    end
  end

  # ── export_rows / export_schema / export_indexes (filesystem) ────────────────

  describe 'filesystem export helpers' do
    let(:dir) { Dir.mktmpdir }
    after { FileUtils.rm_rf(dir) }

    describe '.export_rows' do
      it 'appends NDJSON lines to the data file' do
        FileUtils.mkdir_p(File.join(dir, 'data'))
        rows = { table_name: :users, header: [:id, :name], data: [[1, 'Alice']] }
        described_class.export_rows(dir, :users, rows)
        described_class.export_rows(dir, :users, rows)
        lines = File.readlines(File.join(dir, 'data', 'users.json'))
        expect(lines.size).to eq(2)
        expect(JSON.parse(lines.first)).to have_key('table_name')
      end
    end

    describe '.export_schema' do
      it 'writes the schema string to schemas/<table>.rb' do
        FileUtils.mkdir_p(File.join(dir, 'schemas'))
        described_class.export_schema(dir, :users, 'Class.new(Sequel::Migration) { def up; end }')
        content = File.read(File.join(dir, 'schemas', 'users.rb'))
        expect(content).to include('Sequel::Migration')
      end
    end

    describe '.export_indexes' do
      it 'appends index migration strings as NDJSON' do
        FileUtils.mkdir_p(File.join(dir, 'indexes'))
        described_class.export_indexes(dir, :users, 'add_index :users, :email')
        described_class.export_indexes(dir, :users, 'add_index :users, :name')
        lines = File.readlines(File.join(dir, 'indexes', 'users.json'))
        expect(lines.size).to eq(2)
      end
    end
  end
end
