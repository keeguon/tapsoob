require 'spec_helper'
require 'tapsoob/schema'

RSpec.describe Tapsoob::Schema do
  let(:db) do
    d = connect_sqlite
    d.extension :schema_dumper
    d.create_table(:articles) do
      primary_key :id
      String  :title, null: false, size: 255
      String  :body,  text: true
      DateTime :published_at
    end
    d
  end

  after { db.disconnect }

  # ── dump_table ───────────────────────────────────────────────────────────────

  describe '.dump_table' do
    it 'returns a Sequel migration string' do
      result = described_class.dump_table(db, :articles, {})
      expect(result).to include('Sequel::Migration')
      expect(result).to include('articles')
    end

    it 'accepts a URL string' do
      url = sqlite_memory_url
      Sequel.connect(url) do |tmp|
        tmp.create_table(:t) { primary_key :id; String :v }
        result = described_class.dump_table(url, :t, {})
        expect(result).to include('Sequel::Migration')
      end
    end
  end

  # ── load / round-trip ────────────────────────────────────────────────────────

  describe '.load' do
    it 'creates the table in the destination DB' do
      schema_str = described_class.dump_table(db, :articles, {})

      dest = connect_sqlite
      dest.extension :schema_dumper
      described_class.load(dest, schema_str)
      expect(dest.table_exists?(:articles)).to be true
      dest.disconnect
    end

    it 'drops then recreates table when drop: true' do
      schema_str = described_class.dump_table(db, :articles, {})
      dest = connect_sqlite
      dest.extension :schema_dumper
      described_class.load(dest, schema_str)
      described_class.load(dest, schema_str, drop: true)
      expect(dest.table_exists?(:articles)).to be true
      dest.disconnect
    end
  end

  # ── indexes_individual ───────────────────────────────────────────────────────

  describe '.indexes_individual' do
    let(:indexed_db) do
      d = connect_sqlite
      d.create_table(:idx_test) { primary_key :id; String :email, size: 100 }
      d.add_index(:idx_test, :email)
      d
    end

    after { indexed_db.disconnect }

    it 'returns a JSON string' do
      url = sqlite_memory_url
      Sequel.connect(url) do |tmp|
        tmp.create_table(:t) { primary_key :id; String :v }
        tmp.add_index(:t, :v)
        result = described_class.indexes_individual(url)
        expect { JSON.parse(result) }.not_to raise_error
      end
    end
  end

  # ── reset_db_sequences ───────────────────────────────────────────────────────

  describe '.reset_db_sequences' do
    it 'runs without error on SQLite (which has no sequences)' do
      expect {
        described_class.reset_db_sequences(sqlite_memory_url)
      }.not_to raise_error
    end
  end
end
