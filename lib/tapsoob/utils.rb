# -*- encoding : utf-8 -*-
require 'zlib'

require 'tapsoob/errors'
require 'tapsoob/chunksize'
require 'tapsoob/schema'

module Tapsoob
  module Utils
    extend self

    def windows?
      return @windows if defined?(@windows)
      require 'rbconfig'
      @windows = !!(::RbConfig::CONFIG['host_os'] =~ /mswin|mingw/)
    end

    def bin(cmd)
      cmd = "#{cmd}.cmd" if windows?
      cmd
    end

    def checksum(data)
      Zlib.crc32(data)
    end

    def valid_data?(data, crc32)
      Zlib.crc32(data) == crc32.to_i
    end

    def base64encode(data)
      [data].pack("m")
    end

    def base64decode(data)
      data.unpack("m").first
    end

    def format_data(data, opts = {})
      return {} if data.size == 0
      string_columns = opts[:string_columns] || []
      schema = opts[:schema] || []
      table = opts[:table]

      max_lengths = schema.inject({}) do |hash, (column, meta)|
        if meta[:db_type] =~ /^varchar\((\d+)\)/
          hash.update(column => $1.to_i)
        end
        hash
      end

      header = data[0].keys
      only_data = data.collect do |row|
        row = blobs_to_string(row, string_columns)
        row.each do |column, data|
          if data.to_s.length > (max_lengths[column] || data.to_s.length)
            raise Tapsoob::InvalidData.new(<<-ERROR)
Detected data that exceeds the length limitation of its column. This is
generally due to the fact that SQLite does not enforce length restrictions.

Table : #{table}
Column : #{column}
Type : #{schema.detect{|s| s.first == column}.last[:db_type]}
Data : #{data}
            ERROR
          end

          # Type conversion
          row[column] = data.strftime('%Y-%m-%d %H:%M:%S') if data.is_a?(Time)
        end
        header.collect { |h| row[h] }
      end
      { :header => header, :data => only_data }
    end

    # mysql text and blobs fields are handled the same way internally
    # this is not true for other databases so we must check if the field is
    # actually text and manually convert it back to a string
    def incorrect_blobs(db, table)
      return [] if (db.url =~ /mysql:\/\//).nil?

      columns = []
      db.schema(table).each do |data|
        column, cdata = data
        columns << column if cdata[:db_type] =~ /text/
      end
      columns
    end

    def blobs_to_string(row, columns)
      return row if columns.size == 0
      columns.each do |c|
        row[c] = row[c].to_s if row[c].kind_of?(Sequel::SQL::Blob)
      end
      row
    end

    def calculate_chunksize(old_chunksize)
      c = Tapsoob::Chunksize.new(old_chunksize)

      begin
        c.start_time = Time.now
        c.time_in_db = yield c
      rescue Errno::EPIPE
        c.retries += 1
        raise if c.retries > 2

        # we got disconnected, the chunksize could be too large
        # reset the chunksize based on the number of retries
        c.reset_chunksize
        retry
      end

      c.end_time = Time.now
      c.calc_new_chunksize
    end

    def export_schema(dump_path, table, schema_data)
      File.open(File.join(dump_path, "schemas", "#{table}.rb"), 'w') do |file|
        file.write(schema_data)
      end
    end

    def export_indexes(dump_path, table, index_data)
      data = [index_data]
      if File.exists?(File.join(dump_path, "indexes", "#{table}.json"))
        previous_data = JSON.parse(File.read(File.join(dump_path, "indexes", "#{table}.json")))
        data = data + previous_data
      end

      File.open(File.join(dump_path, "indexes", "#{table}.json"), 'w') do |file|
        file.write(JSON.generate(data))
      end
    end

    def export_rows(dump_path, table, row_data)
      data = row_data
      if File.exists?(File.join(dump_path, "data", "#{table}.json"))
        previous_data = JSON.parse(File.read(File.join(dump_path, "data", "#{table}.json")))
        data[:data] = previous_data["data"] + row_data[:data]
      end

      File.open(File.join(dump_path, "data", "#{table}.json"), 'w') do |file|
        file.write(JSON.generate(data))
      end
    end

    def load_schema(dump_path, database_url, table)
      schema = File.join(dump_path, "schemas", "#{table}.rb")
      schema_bin(:load, database_url, schema.to_s)
    end

    def load_indexes(database_url, index)
      Tapsoob::Schema.load_indexes(database_url, index)
    end

    def schema_bin(*args)
      bin_path = File.expand_path("#{File.dirname(__FILE__)}/../../bin/#{bin('schema')}")
      `"#{bin_path}" #{args.map { |a| "'#{a}'" }.join(' ')}`
    end

    def primary_key(db, table)
      db.schema(table).select { |c| c[1][:primary_key] }.map { |c| c[0] }
    end

    def single_integer_primary_key(db, table)
      table = table.to_sym.identifier unless table.kind_of?(Sequel::SQL::Identifier)
      keys = db.schema(table).select { |c| c[1][:primary_key] and c[1][:type] == :integer }
      not keys.nil? and keys.size == 1
    end

    def order_by(db, table)
      pkey = primary_key(db, table)
      if pkey
        pkey.kind_of?(Array) ? pkey : [pkey.to_sym]
      else
        table = table.to_sym.identifier unless table.kind_of?(Sequel::SQL::Identifier)
        db[table].columns
      end
    end
  end
end
