require 'spec_helper'
require 'tapsoob/data_stream/keyed'

Sequel.extension :core_extensions

RSpec.describe Tapsoob::DataStream::Keyed do
  let(:db) do
    d = connect_sqlite
    d.extension :schema_dumper
    d.create_table(:keyed_items) { primary_key :id; String :label }
    10.times { |i| d[:keyed_items].insert(label: "item_#{i}") }
    d
  end

  after { db.disconnect }

  subject(:stream) { described_class.new(db, { table_name: :keyed_items, chunksize: 4 }) }

  # ── primary_key / buffer_limit ───────────────────────────────────────────────

  describe '#primary_key' do
    it 'returns :id for this table' do
      expect(stream.primary_key).to eq(:id)
    end
  end

  describe '#buffer_limit' do
    it 'returns filter when buffer is non-empty' do
      stream.state[:filter] = 5
      stream.buffer << { id: 1 }
      expect(stream.buffer_limit).to eq(5)
    end

    it 'returns last_fetched when it is less than filter and buffer is empty' do
      stream.state[:filter] = 10
      stream.state[:last_fetched] = 3
      stream.buffer.clear
      expect(stream.buffer_limit).to eq(3)
    end

    it 'returns filter when last_fetched >= filter' do
      stream.state[:filter] = 5
      stream.state[:last_fetched] = 5
      stream.buffer.clear
      expect(stream.buffer_limit).to eq(5)
    end
  end

  # ── calc_limit ───────────────────────────────────────────────────────────────

  describe '#calc_limit' do
    it 'returns chunksize * 3 outside Sinatra' do
      expect(stream.calc_limit(100)).to eq(300)
    end
  end

  # ── load_buffer ──────────────────────────────────────────────────────────────

  describe '#load_buffer' do
    it 'loads rows into the buffer' do
      stream.load_buffer(4)
      expect(stream.buffer.size).to be >= 4
    end

    it 'updates state[:filter] to the last PK in the buffer' do
      stream.load_buffer(4)
      expect(stream.state[:filter]).to eq(stream.buffer.last[:id])
    end

    it 'stops early when the table is exhausted' do
      stream.load_buffer(1000)
      expect(stream.buffer.size).to eq(10)
    end
  end

  # ── fetch_buffered ───────────────────────────────────────────────────────────

  describe '#fetch_buffered' do
    it 'returns up to chunksize rows' do
      rows = stream.fetch_buffered(4)
      expect(rows.size).to eq(4)
    end

    it 'records last_fetched as the PK of the last returned row' do
      rows = stream.fetch_buffered(4)
      expect(stream.state[:last_fetched]).to eq(rows.last[:id])
    end

    it 'returns nil last_fetched when no rows available' do
      # drain the table
      stream.load_buffer(100)
      stream.buffer.clear
      rows = stream.fetch_buffered(4)
      expect(rows).to be_empty
      expect(stream.state[:last_fetched]).to be_nil
    end
  end

  # ── increment ────────────────────────────────────────────────────────────────

  describe '#increment' do
    it 'removes n rows from the front of the buffer' do
      stream.load_buffer(6)
      original_fourth = stream.buffer[3]
      stream.increment(3)
      expect(stream.buffer.first).to eq(original_fourth)
    end
  end

  # ── verify_stream ────────────────────────────────────────────────────────────

  describe '#verify_stream' do
    it 'sets filter to the max PK and clears last_fetched' do
      stream.state[:last_fetched] = 3
      stream.verify_stream
      max_id = db[:keyed_items].max(:id)
      expect(stream.state[:filter]).to eq(max_id)
      expect(stream.state[:last_fetched]).to be_nil
    end
  end
end
