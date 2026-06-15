# -*- encoding : utf-8 -*-
require 'sequel'
require 'sequel/extensions/schema_dumper'
require 'sequel/extensions/migration'
require 'erb'
require 'json'
require 'tapsoob/log'

module Tapsoob
  module Schema
    extend self

    def dump(database_url, options = {})
      Sequel.connect(database_url) do |db|
        db.extension :schema_dumper
        template = ERB.new <<-END_MIG
Class.new(Sequel::Migration) do
  def up
  <% db.send(:sort_dumped_tables, db.tables, {}).each do |table| %>
    <%= db.dump_table_schema(table, options) %>
  <% end %>
  end

  def down
  <% db.send(:sort_dumped_tables, db.tables, {}).reverse.each do |table| %>
    drop_table("<%= table %>", if_exists: true, cascade: true)
  <% end %>
  end
end
END_MIG

        template.result(binding)
      end
    end

    def dump_table(database_url_or_db, table, options)
      table = table.to_sym
      # Accept either a database URL or an existing connection object
      if database_url_or_db.is_a?(Sequel::Database)
        db = database_url_or_db
        db.extension :schema_dumper
        <<END_MIG
Class.new(Sequel::Migration) do
  def up
    #{db.dump_table_schema(table, options)}
  end

  def down
    drop_table("#{table}", if_exists: true)
  end
end
END_MIG
      else
        Sequel.connect(database_url_or_db) do |db|
          db.extension :schema_dumper
          <<END_MIG
Class.new(Sequel::Migration) do
  def up
    #{db.dump_table_schema(table, options)}
  end

  def down
    drop_table("#{table}", if_exists: true)
  end
end
END_MIG
        end
      end
    end

    def foreign_keys(database_url)
      Sequel.connect(database_url) do |db|
        db.extension :schema_dumper
        db.dump_foreign_key_migration
      end
    end

    def indexes(database_url)
      Sequel.connect(database_url) do |db|
        db.extension :schema_dumper
        db.dump_indexes_migration
      end
    end

    def indexes_individual(database_url)
      idxs = {}
      Sequel.connect(database_url) do |db|
        db.extension :schema_dumper

        tables = db.tables
        tables.each do |table|
          idxs[table] = db.send(:dump_table_indexes, table, :add_index, {}).split("\n")
        end
      end

      idxs.each do |table, indexes|
        idxs[table] = indexes.map do |idx|
          <<END_MIG
Class.new(Sequel::Migration) do
  def up
    #{idx}
  end
end
END_MIG
        end
      end
      JSON.generate(idxs)
    end

    def load(database_url_or_db, schema, options = { drop: false })
      schema = rewrite_non_integer_primary_keys(schema)
      # Accept either a database URL or an existing connection object
      if database_url_or_db.is_a?(Sequel::Database)
        db = database_url_or_db
        db.extension :schema_dumper
        klass = eval(schema)
        if options[:drop]
          # Start special hack for MySQL
          db.run("SET foreign_key_checks = 0") if [:mysql, :mysql2].include?(db.adapter_scheme)
          db.run("PRAGMA foreign_keys = OFF") if db.adapter_scheme == :sqlite

          # Run down migration
          klass.apply(db, :down)

          # End special hack for MySQL
          db.run("SET foreign_key_checks = 1") if [:mysql, :mysql2].include?(db.adapter_scheme)
          db.run("PRAGMA foreign_keys = ON") if db.adapter_scheme == :sqlite
        end
        klass.apply(db, :up)
      else
        Sequel.connect(database_url_or_db) do |db|
          db.extension :schema_dumper
          klass = eval(schema)
          if options[:drop]
            # Start special hack for MySQL
            db.run("SET foreign_key_checks = 0") if [:mysql, :mysql2].include?(db.adapter_scheme)
            db.run("PRAGMA foreign_keys = OFF") if db.adapter_scheme == :sqlite

            # Run down migration
            klass.apply(db, :down)

            # End special hack for MySQL
            db.run("SET foreign_key_checks = 1") if [:mysql, :mysql2].include?(db.adapter_scheme)
            db.run("PRAGMA foreign_keys = ON") if db.adapter_scheme == :sqlite
          end
          klass.apply(db, :up)
        end
      end
    end

    def load_foreign_keys(database_url, foreign_keys)
      Sequel.connect(database_url) do |db|
        db.extension :schema_dumper
        eval(foreign_keys).apply(db, :up)
      end
    end

    def load_indexes(database_url, indexes)
      Sequel.connect(database_url) do |db|
        db.extension :schema_dumper
        eval(indexes).apply(db, :up)
      end
    end

    NON_INTEGER_PK_PATTERN = /^(\s*)primary_key\s+(:?\w+),\s*:type=>"([^"]+)"(.*)$/
    INTEGER_DB_TYPES       = /\A(?:int(?:eger|\d+)?|bigint|smallint|serial|bigserial|smallserial)/i

    # On PG 10+, Sequel's CreateTableGenerator injects `identity: true` into
    # every primary_key call via serial_primary_key_options. PG rejects IDENTITY
    # on non-integer types. Rewrite `primary_key :col, :type=>"varchar..."` to
    # `column :col, "varchar...", primary_key: true, null: false` which bypasses
    # that code path entirely.
    def rewrite_non_integer_primary_keys(schema_str)
      schema_str.gsub(NON_INTEGER_PK_PATTERN) do
        indent, col, db_type, rest = $1, $2, $3, $4
        if db_type =~ INTEGER_DB_TYPES
          "#{indent}primary_key #{col}, :type=>\"#{db_type}\"#{rest}"
        else
          "#{indent}column #{col}, \"#{db_type}\", primary_key: true, null: false#{rest}"
        end
      end
    end

    def reset_db_sequences(database_url)
      Sequel.connect(database_url) do |db|
        db.extension :schema_dumper
        next unless db.respond_to?(:reset_primary_key_sequence)
        db.tables.each do |table|
          pk = db.primary_key(table)
          next unless pk
          pk_type = db.schema(table).find { |col, _| col.to_s == pk.to_s }&.last&.dig(:db_type)
          next unless pk_type&.match?(/int|serial/i)
          db.reset_primary_key_sequence(table)
        rescue Sequel::DatabaseError => e
          Tapsoob.log.warn "Could not reset sequence for table '#{table}': #{e.message.lines.first.chomp}"
        end
      end
    end
  end
end
