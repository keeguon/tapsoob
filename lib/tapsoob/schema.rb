# -*- encoding : utf-8 -*-
require 'sequel'
require 'sequel/extensions/schema_dumper'
require 'sequel/extensions/migration'
require 'erb'
require 'json'

module Tapsoob
  module Schema
    extend self

    def dump(database_url)
      db = Sequel.connect(database_url)
      db.extension :schema_dumper
      template = ERB.new <<-END_MIG
Class.new(Sequel::Migration) do
  def up
  <% db.send(:sort_dumped_tables, db.tables).each do |table| %>
    <%= db.dump_table_schema(table, indexes: false) %>
  <% end %>
  end

  def down
  <% db.tables.each do |table| %>
    drop_table("<%= table %>", if_exists: true)
  <% end %>
  end
end
END_MIG

      template.result(binding)
    end

    def dump_table(database_url, table)
      table = table.to_sym
      Sequel.connect(database_url) do |db|
        db.extension :schema_dumper
        <<END_MIG
Class.new(Sequel::Migration) do
  def up
    #{db.dump_table_schema(table, indexes: false)}
  end

  def down
    drop_table("#{table}", if_exists: true)
  end
end
END_MIG
      end
    end

    def indexes(database_url)
      db = Sequel.connect(database_url)
      db.extension :schema_dumper
      db.dump_indexes_migration
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

    def load(database_url, schema, options = { drop: false })
      Sequel.connect(database_url) do |db|
        db.extension :schema_dumper
        klass = eval(schema)
        if options[:drop]
          # Start special hack for MySQL
          db.run("SET foreign_key_checks = 0") if [:mysql, :mysql2].include?(db.adapter_scheme)

          # Run down migration
          klass.apply(db, :down)

          # End special hack for MySQL
          db.run("SET foreign_key_checks = 1") if [:mysql, :mysql2].include?(db.adapter_scheme)
        end
        klass.apply(db, :up)
      end
    end

    def load_indexes(database_url, indexes)
      Sequel.connect(database_url) do |db|
        db.extension :schema_dumper
        eval(indexes).apply(db, :up)
      end
    end

    def reset_db_sequences(database_url)
      db = Sequel.connect(database_url)
      db.extension :schema_dumper
      return unless db.respond_to?(:reset_primary_key_sequence)
      db.tables.each do |table|
        db.reset_primary_key_sequence(table)
      end
    end
  end
end
