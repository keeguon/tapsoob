require 'spec_helper'
require 'tapsoob/schema'

RSpec.describe Tapsoob::Schema do
  # SQLite file-based URL helpers — memory: URLs open a fresh DB on each connect,
  # so methods that open their own connection (dump, foreign_keys, indexes, …)
  # must use a file-backed database.
  def with_sqlite_file
    path = File.join(Dir.tmpdir, "tapsoob_schema_#{Process.pid}_#{rand(9999)}.db")
    url  = DbHelpers.adapt_url("sqlite://#{path}")
    db   = DbHelpers.connect(url)
    db.extension :schema_dumper
    yield url, db
  ensure
    DbHelpers.disconnect_all
    File.delete(path) rescue nil
  end

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
      # sqlite::memory: opens a fresh empty DB on every connect, so dump_table
      # (which opens its own connection) would see no tables. Use a temp file instead.
      require 'tmpdir'
      tmp_path = File.join(Dir.tmpdir, "tapsoob_schema_test_#{Process.pid}.db")
      url = DbHelpers.adapt_url("sqlite://#{tmp_path}")
      begin
        tmp = DbHelpers.connect(url)
        tmp.create_table(:t) { primary_key :id; String :v }
        result = described_class.dump_table(url, :t, {})
        expect(result).to include('Sequel::Migration')
      ensure
        DbHelpers.disconnect_all
        File.delete(tmp_path) rescue nil
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

  # ── dump ─────────────────────────────────────────────────────────────────────

  describe '.dump' do
    it 'returns a migration string covering all tables' do
      with_sqlite_file do |url, tmp|
        tmp.create_table(:things) { primary_key :id; String :name }
        result = described_class.dump(url)
        expect(result).to include('Sequel::Migration')
        expect(result).to include('things')
      end
    end

    it 'includes both up and down blocks' do
      with_sqlite_file do |url, tmp|
        tmp.create_table(:items) { primary_key :id }
        result = described_class.dump(url)
        expect(result).to include('def up')
        expect(result).to include('def down')
      end
    end
  end

  # ── foreign_keys ─────────────────────────────────────────────────────────────

  describe '.foreign_keys' do
    it 'returns a string (even when there are no FK constraints)' do
      with_sqlite_file do |url, _|
        result = described_class.foreign_keys(url)
        expect(result).to be_a(String)
      end
    end
  end

  # ── indexes ──────────────────────────────────────────────────────────────────

  describe '.indexes' do
    it 'returns a string' do
      with_sqlite_file do |url, tmp|
        tmp.create_table(:idx_things) { primary_key :id; String :slug }
        tmp.add_index(:idx_things, :slug)
        result = described_class.indexes(url)
        expect(result).to be_a(String)
      end
    end
  end

  # ── load via URL ─────────────────────────────────────────────────────────────

  describe '.load (URL path)' do
    it 'creates the table when passed a URL string' do
      with_sqlite_file do |url, tmp|
        schema_str = described_class.dump_table(db, :articles, {})
        described_class.load(url, schema_str)
        expect(tmp.table_exists?(:articles)).to be true
      end
    end

    it 'drops then recreates table when drop: true and passed a URL' do
      with_sqlite_file do |url, tmp|
        schema_str = described_class.dump_table(db, :articles, {})
        described_class.load(url, schema_str)
        described_class.load(url, schema_str, drop: true)
        expect(tmp.table_exists?(:articles)).to be true
      end
    end
  end

  # ── load_indexes ─────────────────────────────────────────────────────────────

  describe '.load_indexes' do
    it 'applies an index migration without error' do
      with_sqlite_file do |url, tmp|
        tmp.create_table(:things) { primary_key :id; String :slug }
        index_migration = <<~RUBY
          Class.new(Sequel::Migration) do
            def up
              add_index :things, :slug
            end
          end
        RUBY
        expect { described_class.load_indexes(url, index_migration) }.not_to raise_error
        expect(tmp.indexes(:things)).to have_key(:things_slug_index)
      end
    end
  end

  # ── load_foreign_keys ────────────────────────────────────────────────────────

  describe '.load_foreign_keys' do
    it 'applies a foreign key migration without error' do
      with_sqlite_file do |url, tmp|
        tmp.create_table(:parents) { primary_key :id }
        tmp.create_table(:children) { primary_key :id; Integer :parent_id }
        fk_migration = <<~RUBY
          Class.new(Sequel::Migration) do
            def up
              alter_table(:children) { add_foreign_key [:parent_id], :parents }
            end
          end
        RUBY
        expect { described_class.load_foreign_keys(url, fk_migration) }.not_to raise_error
      end
    end
  end

  # ── rewrite_non_integer_primary_keys ─────────────────────────────────────────

  describe '.rewrite_non_integer_primary_keys' do
    it 'leaves integer primary keys unchanged' do
      schema = '  primary_key :id, :type=>"integer"'
      expect(described_class.rewrite_non_integer_primary_keys(schema)).to eq(schema)
    end

    it 'leaves bigint primary keys unchanged' do
      schema = '  primary_key :id, :type=>"bigint"'
      expect(described_class.rewrite_non_integer_primary_keys(schema)).to eq(schema)
    end

    it 'rewrites varchar primary keys to column form' do
      schema = '  primary_key :code, :type=>"varchar(10)"'
      result = described_class.rewrite_non_integer_primary_keys(schema)
      expect(result).to include('column :code')
      expect(result).to include('"varchar(10)"')
      expect(result).to include('primary_key: true')
      expect(result).not_to include('primary_key :code,')
    end

    it 'rewrites uuid primary keys to column form' do
      schema = '  primary_key :id, :type=>"uuid"'
      result = described_class.rewrite_non_integer_primary_keys(schema)
      expect(result).to include('column :id')
      expect(result).to include('primary_key: true')
    end

    it 'passes through schema with no primary_key lines unchanged' do
      schema = "  String :name, size: 50\n  Integer :score"
      expect(described_class.rewrite_non_integer_primary_keys(schema)).to eq(schema)
    end
  end
end
