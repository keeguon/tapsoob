require 'spec_helper'
require 'tapsoob/data_stream/file_partition'

RSpec.describe Tapsoob::DataStream::FilePartition do
  let(:db) do
    d = connect_sqlite
    d.extension :schema_dumper
    d.create_table(:widgets) { primary_key :id; String :name }
    d
  end

  after { db.disconnect }

  def make_ndjson(dir, table, chunks)
    FileUtils.mkdir_p(File.join(dir, 'data'))
    path = File.join(dir, 'data', "#{table}.json")
    File.open(path, 'w') do |f|
      chunks.each { |chunk| f.puts(JSON.generate(chunk)) }
    end
    path
  end

  def new_stream(state_overrides = {})
    base_state = { table_name: :widgets, chunksize: 10 }
    described_class.new(db, base_state.merge(state_overrides))
  end

  # ── initialize ────────────────────────────────────────────────────────────────

  describe '#initialize' do
    it 'sets current_line to start_line when line_range is provided' do
      stream = new_stream(line_range: [3, 7])
      expect(stream.state[:current_line]).to eq(3)
    end

    it 'leaves current_line unset when no line_range' do
      stream = new_stream
      expect(stream.state[:current_line]).to be_nil
    end
  end

  # ── complete? ────────────────────────────────────────────────────────────────

  describe '#complete?' do
    it 'returns true when line_range is nil' do
      expect(new_stream.complete?).to be true
    end

    it 'returns false when current_line is at start' do
      stream = new_stream(line_range: [0, 4])
      expect(stream.complete?).to be false
    end

    it 'returns true when current_line exceeds end_line' do
      stream = new_stream(line_range: [0, 2], current_line: 3)
      expect(stream.complete?).to be true
    end
  end

  # ── fetch_file ───────────────────────────────────────────────────────────────

  describe '#fetch_file' do
    let(:dir) { Dir.mktmpdir }
    after { FileUtils.rm_rf(dir) }

    it 'returns {} when line_range is nil' do
      stream = new_stream
      expect(stream.fetch_file(dir)).to eq({})
    end

    it 'reads rows within the assigned line range' do
      chunks = [
        { "table_name" => "widgets", "header" => ["id", "name"], "types" => ["integer", "string"], "data" => [[1, "a"]] },
        { "table_name" => "widgets", "header" => ["id", "name"], "types" => ["integer", "string"], "data" => [[2, "b"]] },
        { "table_name" => "widgets", "header" => ["id", "name"], "types" => ["integer", "string"], "data" => [[3, "c"]] },
      ]
      make_ndjson(dir, :widgets, chunks)

      stream = new_stream(line_range: [0, 1])
      result = stream.fetch_file(dir)

      expect(result[:table_name]).to eq("widgets")
      expect(result[:header]).to eq(["id", "name"])
      expect(result[:data]).to eq([[1, "a"], [2, "b"]])
    end

    it 'advances current_line after each fetch' do
      chunks = 3.times.map { |i| { "table_name" => "widgets", "header" => ["id"], "types" => ["integer"], "data" => [[i]] } }
      make_ndjson(dir, :widgets, chunks)

      stream = new_stream(line_range: [0, 2], chunksize: 2)
      stream.fetch_file(dir)
      expect(stream.state[:current_line]).to eq(2)
    end

    it 'marks complete after reading all lines in range' do
      chunks = 2.times.map { |i| { "table_name" => "widgets", "header" => ["id"], "types" => ["integer"], "data" => [[i]] } }
      make_ndjson(dir, :widgets, chunks)

      stream = new_stream(line_range: [0, 1], chunksize: 10)
      stream.fetch_file(dir)
      expect(stream.complete?).to be true
    end

    it 'respects skip-duplicates option' do
      chunks = [
        { "table_name" => "widgets", "header" => ["id"], "types" => ["integer"], "data" => [[1], [1], [2]] },
      ]
      make_ndjson(dir, :widgets, chunks)

      stream = described_class.new(db, { table_name: :widgets, chunksize: 10, line_range: [0, 0] }, { "skip-duplicates": true })
      result = stream.fetch_file(dir)
      expect(result[:data].uniq).to eq(result[:data])
    end
  end
end
