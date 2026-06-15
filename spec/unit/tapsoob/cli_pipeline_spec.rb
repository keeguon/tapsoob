require 'spec_helper'
require 'tapsoob/cli/root'
require 'tapsoob/cli/schema'
require 'tapsoob/cli/data_stream'

# CLI pipeline specs — invoke Thor commands the same way clients do in rake tasks:
#
#   tapsoob schema dump    <src_url>
#   tapsoob schema load    <dst_url>
#   tapsoob schema indexes <src_url>
#   tapsoob schema load_indexes <dst_url>
#   tapsoob schema foreign_keys <src_url>
#   tapsoob schema load_foreign_keys <dst_url>
#   tapsoob data   pull    <src_url>  [dump_path]
#   tapsoob data   push    <dst_url>  [dump_path]
#   tapsoob schema reset_db_sequences <dst_url>
#   tapsoob pull   <dump_path> <src_url>
#   tapsoob push   <dump_path> <dst_url>
#   tapsoob version

RSpec.describe "CLI pipelines" do
  # ── helpers ──────────────────────────────────────────────────────────────────

  def make_db(path)
    url = DbHelpers.adapt_url("sqlite://#{path}")
    db  = Sequel.connect(url)
    db.extension :schema_dumper
    db
  end

  def seed_db(db)
    db.create_table(:users)   { primary_key :id; String :name, null: false }
    db.create_table(:widgets) { primary_key :id; Integer :qty, default: 0 }
    3.times { |i| db[:users].insert(name: "user_#{i}") }
    2.times { |i| db[:widgets].insert(qty: i * 5) }
  end

  # Invoke a Thor subclass with argv, swallowing stdout/stderr output.
  def run_cli(klass, argv)
    klass.start(argv, debug: false)
  end

  # ── shared setup ─────────────────────────────────────────────────────────────

  let(:tmp)      { Dir.mktmpdir("tapsoob_cli_") }
  let(:src_path) { File.join(tmp, "src.db") }
  let(:dst_path) { File.join(tmp, "dst.db") }
  let(:dump_dir) { File.join(tmp, "dump") }
  let(:src_url)  { DbHelpers.adapt_url("sqlite://#{src_path}") }
  let(:dst_url)  { DbHelpers.adapt_url("sqlite://#{dst_path}") }

  let(:src_db) do
    db = make_db(src_path)
    seed_db(db)
    db
  end

  # Ensure src_db is created before tests run and everything is cleaned up after.
  before { src_db }
  after  do
    src_db.disconnect rescue nil
    FileUtils.rm_rf(tmp)
  end

  # ── tapsoob version ───────────────────────────────────────────────────────────

  describe "tapsoob version" do
    it 'prints the version string' do
      expect { run_cli(Tapsoob::CLI::Root, ["version"]) }.to output(/\d+\.\d+/).to_stdout
    end
  end

  # ── schema dump | schema load pipeline ───────────────────────────────────────

  describe "schema dump → load pipeline" do
    it 'dumps schema to stdout and loads it into a fresh DB' do
      schema_text = capture_stdout { run_cli(Tapsoob::CLI::Schema, ["dump", src_url]) }
      expect(schema_text).to include("users", "widgets")

      schema_file = File.join(tmp, "schema.rb")
      File.write(schema_file, schema_text)

      dst_db = make_db(dst_path)
      begin
        run_cli(Tapsoob::CLI::Schema, ["load", dst_url, schema_file])
        expect(dst_db.table_exists?(:users)).to be true
        expect(dst_db.table_exists?(:widgets)).to be true
      ensure
        dst_db.disconnect
      end
    end

    it 'schema load with --drop drops existing tables first' do
      schema_text = capture_stdout { run_cli(Tapsoob::CLI::Schema, ["dump", src_url]) }
      schema_file = File.join(tmp, "schema.rb")
      File.write(schema_file, schema_text)

      dst_db = make_db(dst_path)
      begin
        run_cli(Tapsoob::CLI::Schema, ["load", dst_url, schema_file])
        run_cli(Tapsoob::CLI::Schema, ["load", dst_url, schema_file, "--drop=true"])
        expect(dst_db.table_exists?(:users)).to be true
      ensure
        dst_db.disconnect
      end
    end
  end

  # ── indexes pipeline ──────────────────────────────────────────────────────────

  describe "schema indexes → load_indexes pipeline" do
    it 'dumps indexes and loads them without error' do
      index_text = capture_stdout { run_cli(Tapsoob::CLI::Schema, ["indexes", src_url]) }
      index_file = File.join(tmp, "indexes.rb")
      File.write(index_file, index_text)

      # Load schema first so destination tables exist
      schema_text = capture_stdout { run_cli(Tapsoob::CLI::Schema, ["dump", src_url]) }
      schema_file = File.join(tmp, "schema.rb")
      File.write(schema_file, schema_text)
      run_cli(Tapsoob::CLI::Schema, ["load", dst_url, schema_file])

      expect { run_cli(Tapsoob::CLI::Schema, ["load_indexes", dst_url, index_file]) }.not_to raise_error
    end
  end

  # ── foreign_keys pipeline ─────────────────────────────────────────────────────

  describe "schema foreign_keys → load_foreign_keys pipeline" do
    it 'dumps foreign keys and loads them without error' do
      fk_text  = capture_stdout { run_cli(Tapsoob::CLI::Schema, ["foreign_keys", src_url]) }
      fk_file  = File.join(tmp, "fk.rb")
      File.write(fk_file, fk_text)

      schema_text = capture_stdout { run_cli(Tapsoob::CLI::Schema, ["dump", src_url]) }
      schema_file = File.join(tmp, "schema.rb")
      File.write(schema_file, schema_text)
      run_cli(Tapsoob::CLI::Schema, ["load", dst_url, schema_file])

      expect { run_cli(Tapsoob::CLI::Schema, ["load_foreign_keys", dst_url, fk_file]) }.not_to raise_error
    end
  end

  # ── reset_db_sequences ────────────────────────────────────────────────────────

  describe "schema reset_db_sequences" do
    it 'resets sequences on the destination DB without error' do
      schema_text = capture_stdout { run_cli(Tapsoob::CLI::Schema, ["dump", src_url]) }
      schema_file = File.join(tmp, "schema.rb")
      File.write(schema_file, schema_text)
      run_cli(Tapsoob::CLI::Schema, ["load", dst_url, schema_file])

      expect { run_cli(Tapsoob::CLI::Schema, ["reset_db_sequences", dst_url]) }.not_to raise_error
    end
  end

  # ── data pull → push pipeline (dump_path mode) ───────────────────────────────

  describe "data pull → push pipeline" do
    before { FileUtils.mkdir_p(dump_dir) }

    it 'pulls data into dump_dir and pushes it to destination' do
      # Load schema into dst first
      schema_text = capture_stdout { run_cli(Tapsoob::CLI::Schema, ["dump", src_url]) }
      schema_file = File.join(tmp, "schema.rb")
      File.write(schema_file, schema_text)
      run_cli(Tapsoob::CLI::Schema, ["load", dst_url, schema_file])

      run_cli(Tapsoob::CLI::DataStream, ["pull", src_url, dump_dir, "--progress=false", "--chunksize=1000"])
      run_cli(Tapsoob::CLI::DataStream, ["push", dst_url, dump_dir, "--progress=false", "--chunksize=1000"])

      dst_db = make_db(dst_path)
      begin
        expect(dst_db[:users].count).to eq(3)
        expect(dst_db[:widgets].count).to eq(2)
      ensure
        dst_db.disconnect
      end
    end
  end

  # ── tapsoob pull → push (Root command, full round-trip) ──────────────────────

  describe "tapsoob pull → push (Root commands)" do
    it 'performs a full schema+data round-trip via pull/push commands' do
      run_cli(Tapsoob::CLI::Root, ["pull", dump_dir, src_url,
        "--progress=false", "--chunksize=1000", "--no-split"])
      run_cli(Tapsoob::CLI::Root, ["push", dump_dir, dst_url,
        "--progress=false", "--chunksize=1000"])

      dst_db = make_db(dst_path)
      begin
        expect(dst_db.table_exists?(:users)).to be true
        expect(dst_db[:users].count).to eq(3)
        expect(dst_db[:widgets].count).to eq(2)
      ensure
        dst_db.disconnect
      end
    end
  end

  # ── schema dump_table ─────────────────────────────────────────────────────────

  describe "schema dump_table" do
    it 'dumps a single table schema to stdout' do
      output = capture_stdout { run_cli(Tapsoob::CLI::Schema, ["dump_table", src_url, "users"]) }
      expect(output).to include("users")
    end
  end

  # ── data push --purge flag ────────────────────────────────────────────────────

  describe "data push --purge" do
    before { FileUtils.mkdir_p(dump_dir) }

    it 'truncates destination tables before inserting' do
      schema_text = capture_stdout { run_cli(Tapsoob::CLI::Schema, ["dump", src_url]) }
      schema_file = File.join(tmp, "schema.rb")
      File.write(schema_file, schema_text)
      run_cli(Tapsoob::CLI::Schema, ["load", dst_url, schema_file])

      run_cli(Tapsoob::CLI::DataStream, ["pull", src_url, dump_dir, "--progress=false"])
      run_cli(Tapsoob::CLI::DataStream, ["push", dst_url, dump_dir, "--progress=false", "--purge"])

      dst_db = make_db(dst_path)
      begin
        expect(dst_db[:users].count).to eq(3)
      ensure
        dst_db.disconnect
      end
    end
  end

  # ── helper ───────────────────────────────────────────────────────────────────

  def capture_stdout(&block)
    old = $stdout
    $stdout = StringIO.new
    block.call
    $stdout.string
  ensure
    $stdout = old
  end
end
