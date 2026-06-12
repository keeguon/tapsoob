require 'sequel'
require 'database_cleaner/sequel'

# Helpers included in all :integration and :system examples.
# Provides connection management, table creation, and DatabaseCleaner wiring.
module DbHelpers
  def self.included(base)
    base.instance_eval do
      # One connection per adapter, cached for the suite run.
      let(:src_url) { DbHelpers.adapt_url(ENV.fetch('SRC_DATABASE_URL', 'sqlite://tmp/tapsoob_src_test.db')) }
      let(:dst_url) { DbHelpers.adapt_url(ENV.fetch('DST_DATABASE_URL', 'sqlite://tmp/tapsoob_dst_test.db')) }

      let(:src_db) { DbHelpers.connect(src_url) }
      let(:dst_db) { DbHelpers.connect(dst_url) }

      let(:dump_dir) { Dir.mktmpdir('tapsoob_dump_') }

      after(:each) do
        FileUtils.rm_rf(dump_dir)
      end

      after(:all) do
        DbHelpers.disconnect_all
      end
    end
  end

  # ── URL normalisation ────────────────────────────────────────────────────────
  #
  # CI and local env vars always carry MRI-style URLs (mysql2://, postgres://, sqlite://).
  # Under JRuby, Sequel requires JDBC-style URLs (jdbc:mysql://, jdbc:postgresql://, jdbc:sqlite:).
  # This method rewrites the URL transparently so every caller gets the right scheme.
  #
  # Mapping:
  #   sqlite://path/to/file   → jdbc:sqlite:path/to/file       (JRuby)
  #   sqlite::memory:         → jdbc:sqlite::memory:            (JRuby)
  #   mysql2://host/db        → jdbc:mysql://host/db            (JRuby)
  #   postgres://host/db      → jdbc:postgresql://host/db       (JRuby)
  #   postgresql://host/db    → jdbc:postgresql://host/db       (JRuby)
  #   anything jdbc:*         → left unchanged (already JDBC)
  #
  JRUBY = (RUBY_PLATFORM =~ /java/)

  def self.adapt_url(url)
    return url unless JRUBY
    return url if url.start_with?('jdbc:')

    case url
    when /\Asqlite:(?:\/\/)?(.*)\z/           then "jdbc:sqlite:#{$1}"
    when /\Amysql2?:\/\/(.*)\z/               then "jdbc:mysql://#{$1}"
    when /\Apostgres(?:ql)?:\/\/(.*)\z/       then "jdbc:postgresql://#{$1}"
    else url
    end
  end

  # ── connection pool ──────────────────────────────────────────────────────────

  CONNECTIONS = {}
  CONNECTIONS_MUTEX = Mutex.new

  def self.connect(url)
    CONNECTIONS_MUTEX.synchronize do
      CONNECTIONS[url] ||= begin
        db = Sequel.connect(url, max_connections: 10)
        db.extension :schema_dumper
        db
      end
    end
  end

  def self.disconnect_all
    CONNECTIONS_MUTEX.synchronize do
      CONNECTIONS.each_value(&:disconnect)
      CONNECTIONS.clear
    end
  end

  # ── table lifecycle helpers ──────────────────────────────────────────────────

  def drop_and_create(db, &block)
    db.instance_eval(&block)
  end

  # Truncate every test table on db after each example (faster than drop/create).
  def truncate_tables(db, *tables)
    tables.each do |t|
      next unless db.table_exists?(t)
      if [:mysql, :mysql2].include?(db.adapter_scheme)
        db.run("SET foreign_key_checks = 0")
        db[t].truncate
        db.run("SET foreign_key_checks = 1")
      else
        db[t].truncate(cascade: true)
      end
    end
  end

  # ── generic row-count assertion helpers ─────────────────────────────────────

  def row_count(db, table)
    db[table].count
  end

  def table_exists?(db, table)
    db.table_exists?(table)
  end

  # ── schema helpers ───────────────────────────────────────────────────────────

  # Drop a table if it exists, accounting for FK checks on MySQL.
  def safe_drop(db, table)
    return unless db.table_exists?(table)
    if [:mysql, :mysql2].include?(db.adapter_scheme)
      db.run("SET foreign_key_checks = 0")
      db.drop_table(table)
      db.run("SET foreign_key_checks = 1")
    else
      db.drop_table(table, cascade: true)
    end
  end
end
