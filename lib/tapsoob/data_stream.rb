require 'tapsoob/log'
require 'tapsoob/utils'

module Tapsoob
  class DataStream
    DEFAULT_CHUNKSIZE = 1000

    attr_reader :db, :state

    def initialize(db, state)
      @db = db
      @state = {
        :offset          => 0,
        :avg_chunksize   => 0,
        :num_chunksize   => 0,
        :total_chunksize => 0
      }.merge(state)
      @state[:chunksize] ||= DEFAULT_CHUNKSIZE
      @complete = false
    end

    def log
      Tapsoob.log
    end

    def error=(val)
      state[:error] = val
    end

    def error
      state[:error] || false
    end

    def table_name
      state[:table_name].to_sym
    end

    def table_name_sql
      table_name.identifier
    end

    def to_hash
      state.merge(:klass => self.class.to_s)
    end

    def to_json
      JSON.generate(to_hash)
    end

    def string_columns
      @string_columns ||= Tapsoob::Utils.incorrect_blobs(db, table_name)
    end

    def table
      @table ||= db[table_name_sql]
    end

    def order_by(name=nil)
      @order_by ||= begin
        name ||= table_name
        Tapsoob::Utils.order_by(db, name)
      end
    end

    def increment(row_count)
      state[:offset] += row_count
    end

    # keep a record of the average chunksize within the first few hundred thousand records, after chunksize
    # goes below 100 or maybe if offset is > 1000
    def fetch_rows
      state[:chunksize] = fetch_chunksize
      ds = table.order(*order_by).limit(state[:chunksize], state[:total_chunksize])
      log.debug "DataStream#fetch_rows SQL -> #{ds.sql}"
      rows = Tapsoob::Utils.format_data(ds.all,
        :string_columns => string_columns,
        :schema => db.schema(table_name),
        :table => table_name
      )
      update_chunksize_stats
      rows
    end

    def max_chunksize_training
      20
    end

    def fetch_chunksize
      chunksize = state[:chunksize]
      return chunksize if state[:num_chunksize] < max_chunksize_training
      return chunksize if state[:avg_chunksize] == 0
      return chunksize if state[:error]
      state[:avg_chunksize] > chunksize ? state[:avg_chunksize] : chunksize
    end

    def update_chunksize_stats
      return if state[:num_chunksize] >= max_chunksize_training
      state[:total_chunksize] += state[:chunksize]
      state[:num_chunksize] += 1
      state[:avg_chunksize] = state[:total_chunksize] / state[:num_chunksize] rescue state[:chunksize]
    end

    def encode_rows(rows)
      Tapsoob::Utils.base64encode(Marshal.dump(rows))
    end

    def fetch
      log.debug "DataStream#fetch state -> #{state.inspect}"

      t1 = Time.now
      rows = fetch_rows
      encoded_data = encode_rows(rows)
      t2 = Time.now
      elapsed_time = t2 - t1

      @complete = rows == { }

      [encoded_data, (@complete ? 0 : rows[:data].size), elapsed_time]
    end

    def complete?
      @complete
    end

    def self.parse_json(json)
      hash = JSON.parse(json).symbolize_keys
      hash[:state].symbolize_keys! if hash.has_key?(:state)
      hash
    end

    def parse_encoded_data(encoded_data, checksum)
      raise Tapsoob::CorruptedData.new("Checksum Failed") unless Tapsoob::Utils.valid_data?(encoded_data, checksum)

      begin
        return Marshal.load(Tapsoob::Utils.base64decode(encoded_data))
      rescue Object => e
        unless ENV['NO_DUMP_MARSHAL_ERRORS']
          puts "Error encountered loading data, wrote the data chunk to dump.#{Process.pid}.dat"
          File.open("dump.#{Process.pid}.dat", "w") { |f| f.write(encoded_data) }
        end
        raise e
      end
    end

    def verify_stream
      state[:offset] = table.count
    end

    def self.factory(db, state)
      if defined?(Sequel::MySQL) && Sequel::MySQL.respond_to?(:convert_invalid_date_time=)
        Sequel::MySQL.convert_invalid_date_time = :nil
      end

      if state.has_key?(:klass)
        return eval(state[:klass]).new(db, state)
      end

      if Tapsoob::Utils.single_integer_primary_key(db, state[:table_name].to_sym)
        DataStreamKeyed.new(db, state)
      else
        DataStream.new(db, state)
      end
    end
  end
end
