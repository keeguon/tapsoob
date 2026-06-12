# Shared examples that any adapter-specific integration suite can include.
# The including example group must define before(:all) that sets:
#   @src_url, @dst_url  — Sequel connection URLs
#   @src_db, @dst_db    — connected Sequel::Database objects
# Individual examples access these via the src_url/dst_url/src_db/dst_db helpers
# defined in DbHelpers (which delegate to the ivars set in before(:all)).

RSpec.shared_examples 'a complete round-trip' do
  it 'pulls without error' do
    expect { pull(src_url, dump_dir) }.not_to raise_error
  end

  it 'creates schema dump files for every table' do
    pull(src_url, dump_dir)
    src_db.tables.each do |table|
      expect(File).to exist(File.join(dump_dir, 'schemas', "#{table}.rb"))
    end
  end

  it 'creates data dump files for every seeded table' do
    pull(src_url, dump_dir)
    %i[users orders products documents attachments events large_table null_heavy].each do |table|
      expect(File).to exist(File.join(dump_dir, 'data', "#{table}.json"))
    end
  end

  it 'pushes without error' do
    pull(src_url, dump_dir)
    expect { push(dst_url, dump_dir) }.not_to raise_error
  end

  it 'preserves row counts for all tables' do
    round_trip(src_url, dst_url, dump_dir)
    expect_same_counts(src_db, dst_db)
  end

  it 'preserves NULL values in null_heavy' do
    round_trip(src_url, dst_url, dump_dir)
    null_rows = dst_db[:null_heavy].where(maybe_name: nil).count
    expect(null_rows).to be > 0
  end

  it 'preserves string content in users.email' do
    round_trip(src_url, dst_url, dump_dir)
    src_emails = src_db[:users].select_map(:email).sort
    dst_emails = dst_db[:users].select_map(:email).sort
    expect(dst_emails).to eq(src_emails)
  end

  it 'preserves BLOB payloads in attachments' do
    round_trip(src_url, dst_url, dump_dir)
    src_db[:attachments].order(:id).each do |src_row|
      dst_row = dst_db[:attachments][id: src_row[:id]]
      expect(dst_row).not_to be_nil
      expect(dst_row[:payload].to_s.bytes).to eq(src_row[:payload].to_s.bytes)
    end
  end

  it 'preserves large TEXT bodies in documents' do
    round_trip(src_url, dst_url, dump_dir)
    src_db[:documents].order(:id).each do |src_row|
      dst_row = dst_db[:documents][id: src_row[:id]]
      expect(dst_row[:body]).to eq(src_row[:body])
    end
  end

  it 'handles the no-PK events table' do
    round_trip(src_url, dst_url, dump_dir)
    expect(dst_db[:events].count).to eq(src_db[:events].count)
  end
end

RSpec.shared_examples 'a parallel round-trip' do |workers:|
  it "preserves row counts with #{workers} parallel workers" do
    round_trip(src_url, dst_url, dump_dir, parallel: workers)
    expect_same_counts(src_db, dst_db)
  end

  it "handles the large_table (>100K rows) with #{workers} workers" do
    round_trip(src_url, dst_url, dump_dir, parallel: workers)
    expect(dst_db[:large_table].count).to eq(Fixtures::LARGE_TABLE_ROWS)
  end
end
