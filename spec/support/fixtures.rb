require 'faker'
require 'sequel'

# Fixtures.setup_source(db) creates all test tables and seeds them with
# realistic data. Each table targets a distinct edge-case scenario.
#
# Tables created:
#   users          – common mixed-type table (name, email, timestamps)
#   orders         – FK relationship to users, decimal amounts
#   products       – nullable fields, boolean flag, float price
#   documents      – large TEXT body (up to 64 KB per row)
#   attachments    – BLOB payloads (up to 256 KB per row)
#   events         – no primary key (exercises Base stream, not Keyed)
#   large_table    – 150 000+ integer rows (triggers intra-table parallelization)
#   null_heavy     – every nullable column is NULL for half the rows
#
module Fixtures
  LARGE_TABLE_ROWS   = 150_000
  DOCUMENT_ROWS      = 500
  ATTACHMENT_ROWS    = 200
  STANDARD_ROWS      = 1_000
  NULL_HEAVY_ROWS    = 400

  # ── schema ───────────────────────────────────────────────────────────────────

  def self.create_tables(db)
    db.create_table!(:users) do
      primary_key :id
      String  :name,       null: false, size: 100
      String  :email,      null: false, size: 255
      String  :locale,     size: 10
      Integer :age
      Date    :birthday
      DateTime :created_at
      DateTime :updated_at
    end

    db.create_table!(:orders) do
      primary_key :id
      foreign_key :user_id, :users, null: false
      String  :reference,  null: false, size: 50
      Float   :amount
      String  :status,     size: 20,    default: 'pending'
      DateTime :placed_at
    end

    db.create_table!(:products) do
      primary_key :id
      String  :sku,        null: false, size: 50
      String  :name,       null: false, size: 200
      Float   :price
      Integer :stock,      default: 0
      TrueClass :available, default: true
      String  :description, text: true
    end

    db.create_table!(:documents) do
      primary_key :id
      String  :title,      null: false, size: 255
      String  :body,       text: true
      String  :author,     size: 100
      DateTime :published_at
    end

    db.create_table!(:attachments) do
      primary_key :id
      String  :filename,   null: false, size: 255
      String  :mime_type,  size: 100
      Integer :size_bytes
      File    :payload
    end

    db.create_table!(:events) do
      String  :event_type, null: false, size: 50
      String  :actor,      size: 100
      String  :target,     size: 100
      DateTime :occurred_at
    end

    db.create_table!(:large_table) do
      primary_key :id
      String  :data,       null: false, size: 100
      Integer :sequence
      Float   :value
    end

    db.create_table!(:null_heavy) do
      primary_key :id
      String  :maybe_name,   size: 100
      Integer :maybe_number
      Float   :maybe_score
      Date    :maybe_date
      String  :maybe_text,   text: true
    end
  end

  def self.drop_tables(db)
    # Drop in reverse FK order
    [:null_heavy, :large_table, :events, :attachments,
     :documents, :products, :orders, :users].each do |t|
      db.drop_table(t, if_exists: true)
    end
  end

  # ── seeding ──────────────────────────────────────────────────────────────────

  def self.seed(db)
    seed_users(db)
    seed_orders(db)
    seed_products(db)
    seed_documents(db)
    seed_attachments(db)
    seed_events(db)
    seed_large_table(db)
    seed_null_heavy(db)
  end

  # ── users ────────────────────────────────────────────────────────────────────
  # Realistic person records: names from multiple locales, valid emails,
  # age range 18–80, random birthdays, ISO timestamps.

  def self.seed_users(db)
    Faker::Config.locale = :en
    rows = STANDARD_ROWS.times.map do
      now = Faker::Time.between(from: DateTime.new(2020, 1, 1), to: DateTime.now)
      {
        name:       Faker::Name.name,
        email:      Faker::Internet.unique.email,
        locale:     %w[en fr de es ja pt it nl].sample,
        age:        Faker::Number.between(from: 18, to: 80),
        birthday:   Faker::Date.birthday(min_age: 18, max_age: 80).to_s,
        created_at: now.strftime('%Y-%m-%d %H:%M:%S'),
        updated_at: now.strftime('%Y-%m-%d %H:%M:%S')
      }
    end
    db[:users].multi_insert(rows)
    Faker::UniqueGenerator.clear
  end

  # ── orders ───────────────────────────────────────────────────────────────────
  # Each order references a real user. Amounts use realistic e-commerce prices
  # (0.99 – 9999.99). Statuses mirror a typical order lifecycle.

  STATUSES = %w[pending processing shipped delivered cancelled refunded].freeze

  def self.seed_orders(db)
    user_ids = db[:users].select_map(:id)
    rows = STANDARD_ROWS.times.map do
      placed = Faker::Time.between(from: DateTime.new(2022, 1, 1), to: DateTime.now)
      {
        user_id:    user_ids.sample,
        reference:  "ORD-#{Faker::Alphanumeric.unique.alphanumeric(number: 10).upcase}",
        amount:     (Faker::Commerce.price(range: 0.99..9999.99) * 100).round / 100.0,
        status:     STATUSES.sample,
        placed_at:  placed.strftime('%Y-%m-%d %H:%M:%S')
      }
    end
    db[:orders].multi_insert(rows)
    Faker::UniqueGenerator.clear
  end

  # ── products ─────────────────────────────────────────────────────────────────
  # SKUs follow a realistic pattern (3-letter category + 6-digit number).
  # Descriptions are nullable Markdown-ish paragraphs (~200–800 chars).

  def self.seed_products(db)
    categories = %w[ELC CLT FRN SPT HOM KIT OFC]
    rows = STANDARD_ROWS.times.map do
      {
        sku:         "#{categories.sample}-#{Faker::Number.number(digits: 6)}",
        name:        Faker::Commerce.product_name,
        price:       (Faker::Commerce.price * 100).round / 100.0,
        stock:       Faker::Number.between(from: 0, to: 5_000),
        available:   [true, true, true, false].sample,
        description: [nil, Faker::Lorem.paragraphs(number: rand(1..3)).join("\n\n")].sample
      }
    end
    db[:products].multi_insert(rows)
  end

  # ── documents ────────────────────────────────────────────────────────────────
  # Bodies range from a few hundred bytes up to ~64 KB to stress TEXT columns
  # and the adaptive chunksize logic.

  LOREM_BASE = Faker::Lorem.characters(number: 1_000)

  def self.seed_documents(db)
    rows = DOCUMENT_ROWS.times.map do
      target_bytes = [256, 1_024, 4_096, 16_384, 65_536].sample
      body = (LOREM_BASE * ((target_bytes / LOREM_BASE.size) + 2))[0, target_bytes]
      published = Faker::Time.between(from: DateTime.new(2010, 1, 1), to: DateTime.now)
      {
        title:        Faker::Lorem.sentence(word_count: rand(4..10)).chomp('.'),
        body:         body,
        author:       Faker::Name.name,
        published_at: published.strftime('%Y-%m-%d %H:%M:%S')
      }
    end
    db[:documents].multi_insert(rows)
  end

  # ── attachments ──────────────────────────────────────────────────────────────
  # Binary payloads: realistic mix of small thumbnails (1–4 KB), medium images
  # (~50 KB), and occasional large files (~256 KB). Uses Random.bytes so the
  # data is genuinely binary and exercises base64 encode/decode faithfully.

  MIME_TYPES = {
    'image/png'       => '.png',
    'image/jpeg'      => '.jpg',
    'application/pdf' => '.pdf',
    'application/zip' => '.zip',
    'video/mp4'       => '.mp4'
  }.freeze

  def self.seed_attachments(db)
    rows = ATTACHMENT_ROWS.times.map do
      size = [
        rand(1_024..4_096),       # thumbnail  (1–4 KB)
        rand(40_000..60_000),     # image      (~50 KB)
        rand(200_000..262_144)    # large file (~256 KB)
      ].sample
      mime, ext = MIME_TYPES.to_a.sample
      {
        filename:   "#{Faker::File.file_name(dir: '', ext: ext.delete('.'))}",
        mime_type:  mime,
        size_bytes: size,
        payload:    Sequel::SQL::Blob.new(Random.bytes(size))
      }
    end
    db[:attachments].multi_insert(rows)
  end

  # ── events ───────────────────────────────────────────────────────────────────
  # No primary key — exercises DataStream::Base (non-keyed path) for both
  # pull and push.

  EVENT_TYPES = %w[login logout purchase refund view search click signup].freeze

  def self.seed_events(db)
    rows = STANDARD_ROWS.times.map do
      {
        event_type:  EVENT_TYPES.sample,
        actor:       Faker::Internet.username,
        target:      "/#{Faker::Internet.slug}",
        occurred_at: Faker::Time.between(
          from: DateTime.new(2023, 1, 1),
          to:   DateTime.now
        ).strftime('%Y-%m-%d %H:%M:%S')
      }
    end
    db[:events].multi_insert(rows)
  end

  # ── large_table ──────────────────────────────────────────────────────────────
  # 150 000 rows to push past the 100 K intra-table parallelization threshold.
  # Inserted in batches of 5 000 to keep memory low.

  LARGE_TABLE_BATCH = 5_000

  def self.seed_large_table(db)
    total = LARGE_TABLE_ROWS
    batches = (total.to_f / LARGE_TABLE_BATCH).ceil
    batches.times do |b|
      count = [LARGE_TABLE_BATCH, total - b * LARGE_TABLE_BATCH].min
      rows = count.times.map do |i|
        seq = b * LARGE_TABLE_BATCH + i
        {
          data:     Faker::Lorem.characters(number: rand(20..80)),
          sequence: seq,
          value:    rand * 10_000.0
        }
      end
      db[:large_table].multi_insert(rows)
    end
  end

  # ── null_heavy ───────────────────────────────────────────────────────────────
  # Half the rows have every nullable column set to NULL.
  # Ensures NULL survives Marshal encode/decode and DB round-trips.

  def self.seed_null_heavy(db)
    rows = NULL_HEAVY_ROWS.times.map do |i|
      if i.even?
        { maybe_name: nil, maybe_number: nil, maybe_score: nil,
          maybe_date: nil, maybe_text: nil }
      else
        {
          maybe_name:   Faker::Name.name,
          maybe_number: rand(-1_000_000..1_000_000),
          maybe_score:  rand * 100.0,
          maybe_date:   Faker::Date.between(from: '2000-01-01', to: '2030-12-31').to_s,
          maybe_text:   Faker::Lorem.paragraph
        }
      end
    end
    db[:null_heavy].multi_insert(rows)
  end
end
