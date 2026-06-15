# Shared examples that any adapter-specific integration suite can include.
# The including example group must define before(:all) that sets:
#   @src_url, @dst_url  — Sequel connection URLs
#   @src_db, @dst_db    — connected Sequel::Database objects
# Individual examples access these via the src_url/dst_url/src_db/dst_db helpers
# defined in DbHelpers (which delegate to the ivars set in before(:all)).

RSpec.shared_examples 'a complete round-trip' do
  # Pull once into a shared dir and reuse across all examples in this group.
  before(:all) do
    @shared_dump_dir = Dir.mktmpdir('tapsoob_shared_')
    pull(src_url, @shared_dump_dir)
  end

  after(:all) do
    FileUtils.rm_rf(@shared_dump_dir)
  end

  it 'pulls without error' do
    expect(File).to exist(File.join(@shared_dump_dir, 'schemas'))
  end

  it 'creates schema dump files for every table' do
    src_db.tables.each do |table|
      expect(File).to exist(File.join(@shared_dump_dir, 'schemas', "#{table}.rb"))
    end
  end

  it 'creates data dump files for every seeded table' do
    %i[users orders products documents attachments events large_table null_heavy].each do |table|
      expect(File).to exist(File.join(@shared_dump_dir, 'data', "#{table}.json"))
    end
  end

  it 'pushes without error' do
    expect { push(dst_url, @shared_dump_dir) }.not_to raise_error
  end

  it 'preserves row counts for all tables' do
    push(dst_url, @shared_dump_dir)
    expect_same_counts(src_db, dst_db)
  end

  it 'preserves NULL values in null_heavy' do
    push(dst_url, @shared_dump_dir)
    expect(dst_db[:null_heavy].where(maybe_name: nil).count).to be > 0
  end

  it 'preserves string content in users.email' do
    push(dst_url, @shared_dump_dir)
    expect(dst_db[:users].select_map(:email).sort).to eq(src_db[:users].select_map(:email).sort)
  end

  it 'preserves BLOB payloads in attachments' do
    push(dst_url, @shared_dump_dir)
    src_db[:attachments].order(:id).each do |src_row|
      dst_row = dst_db[:attachments][id: src_row[:id]]
      expect(dst_row).not_to be_nil
      expect(dst_row[:payload].to_s.bytes).to eq(src_row[:payload].to_s.bytes)
    end
  end

  it 'preserves large TEXT bodies in documents' do
    push(dst_url, @shared_dump_dir)
    src_db[:documents].order(:id).each do |src_row|
      dst_row = dst_db[:documents][id: src_row[:id]]
      expect(dst_row[:body]).to eq(src_row[:body])
    end
  end

  it 'handles the no-PK events table' do
    push(dst_url, @shared_dump_dir)
    expect(dst_db[:events].count).to eq(src_db[:events].count)
  end
end

RSpec.shared_examples 'a parallel round-trip' do |workers:|
  # Pull once, push with parallel workers.
  before(:all) do
    @parallel_dump_dir = Dir.mktmpdir('tapsoob_parallel_')
    pull(src_url, @parallel_dump_dir, parallel: workers)
  end

  after(:all) do
    FileUtils.rm_rf(@parallel_dump_dir)
  end

  it "preserves row counts with #{workers} parallel workers" do
    push(dst_url, @parallel_dump_dir, parallel: workers)
    expect_same_counts(src_db, dst_db)
  end

  it "handles the large_table with #{workers} workers" do
    push(dst_url, @parallel_dump_dir, parallel: workers)
    expect(dst_db[:large_table].count).to eq(Fixtures::LARGE_TABLE_ROWS)
  end
end
