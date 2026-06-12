require 'tapsoob/operation/pull'
require 'tapsoob/operation/push'

# Helpers for running pull and push operations in specs.
# Included automatically in :integration and :system examples.
module RoundTripHelper
  DEFAULT_OPTS = {
    data:              true,
    schema:            true,
    indexes:           true,
    indexes_first:     false,
    progress:          false,
    default_chunksize: 1000
  }.freeze

  # Run a full pull from +url+ into +dir+.
  def pull(url, dir, opts = {})
    op = Tapsoob::Operation::Pull.new(url, dir, DEFAULT_OPTS.merge(opts))
    op.run
    op
  end

  # Run a full push from +dir+ into +url+.
  def push(url, dir, opts = {})
    op = Tapsoob::Operation::Push.new(url, dir, DEFAULT_OPTS.merge(opts))
    op.run
    op
  end

  # Pull from +src_url+ into +dir+, then push from +dir+ into +dst_url+.
  # Returns [pull_op, push_op].
  def round_trip(src_url, dst_url, dir, opts = {})
    p = pull(src_url, dir, opts)
    q = push(dst_url, dir, opts)
    [p, q]
  end

  # Assert that every table present in src_db also exists in dst_db with the
  # same number of rows.
  def expect_same_counts(src_db, dst_db)
    src_db.tables.each do |table|
      expect(dst_db.table_exists?(table)).to be(true),
        "expected table #{table} to exist in destination"
      src_count = src_db[table].count
      dst_count = dst_db[table].count
      expect(dst_count).to eq(src_count),
        "row count mismatch for #{table}: src=#{src_count} dst=#{dst_count}"
    end
  end

  # Assert that every row in src_db[table] has an identical row in dst_db[table]
  # when compared by ordered primary key. Only usable for small tables.
  def expect_identical_rows(src_db, dst_db, table, order_col: :id)
    src_rows = src_db[table].order(order_col).all
    dst_rows = dst_db[table].order(order_col).all
    expect(dst_rows.size).to eq(src_rows.size)
    src_rows.zip(dst_rows).each_with_index do |(src, dst), i|
      src.each do |col, val|
        if val.is_a?(String) && val.encoding == Encoding::ASCII_8BIT
          # binary – compare as bytes
          expect(dst[col].to_s.bytes).to eq(val.bytes),
            "blob mismatch in #{table}[#{i}].#{col}"
        else
          expect(dst[col]).to eq(val),
            "value mismatch in #{table}[#{i}].#{col}: expected #{val.inspect}, got #{dst[col].inspect}"
        end
      end
    end
  end
end
