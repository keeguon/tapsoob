require 'tapsoob/operation/pull'
require 'tapsoob/operation/push'

# Shared helpers for unit specs that exercise Pull / Push / Base.
# Included automatically via spec_helper for all unit specs.
module OperationHelpers
  # Default opts used across pull/push/base unit tests.
  UNIT_OPTS = {
    data:              true,
    schema:            true,
    indexes:           false,
    progress:          false,
    default_chunksize: 1000,
    no_split:          true,
  }.freeze

  # A pre-seeded in-memory SQLite DB with :users (5 rows) and :widgets (3 rows).
  # Returns a new connection each call — callers own disconnection.
  def seeded_sqlite_db
    d = connect_sqlite
    d.create_table(:users)   { primary_key :id; String :name }
    d.create_table(:widgets) { primary_key :id; Integer :qty }
    5.times { |i| d[:users].insert(name: "user_#{i}") }
    3.times { |i| d[:widgets].insert(qty: i * 10) }
    d
  end

  def build_pull(db, dump_dir, extra_opts = {})
    op = Tapsoob::Operation::Pull.new(sqlite_memory_url, dump_dir, UNIT_OPTS.merge(extra_opts))
    op.instance_variable_set(:@db, db)
    op
  end

  def build_push(db_url, dump_dir, extra_opts = {})
    Tapsoob::Operation::Push.new(db_url, dump_dir, UNIT_OPTS.merge(extra_opts))
  end

  def build_base(db, dump_dir, extra_opts = {})
    op = Tapsoob::Operation::Pull.new(sqlite_memory_url, dump_dir, UNIT_OPTS.merge(extra_opts))
    op.instance_variable_set(:@db, db)
    op
  end
end
