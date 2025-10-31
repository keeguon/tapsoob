# -*- encoding : utf-8 -*-
require 'tapsoob/data_stream/base'

module Tapsoob
  module DataStream
    # DataStream variant for PK-based range partitioning
    class KeyedPartition < Base
      def initialize(db, state, opts = {})
        super(db, state, opts)
        # :partition_range = [min_pk, max_pk] for this partition
        # :last_pk = last primary key value fetched
        @state = {
          :partition_range => nil,
          :last_pk => nil
        }.merge(@state)
      end

      def primary_key
        @primary_key ||= Tapsoob::Utils.order_by(db, table_name).first
      end

      def fetch_rows
        return {} if state[:partition_range].nil?

        # Only count once on first fetch
        state[:size] ||= table.count

        min_pk, max_pk = state[:partition_range]
        chunksize = state[:chunksize]

        # Build query with PK range filter
        key = primary_key
        last = state[:last_pk] || (min_pk - 1)

        ds = table.order(*order_by).filter do
          (Sequel.identifier(key) > last) & (Sequel.identifier(key) >= min_pk) & (Sequel.identifier(key) <= max_pk)
        end.limit(chunksize)

        data = ds.all

        # Update last_pk for next fetch
        if data.any?
          state[:last_pk] = data.last[primary_key]
        else
          # No data found in this range - mark partition as complete
          state[:last_pk] = max_pk
        end

        Tapsoob::Utils.format_data(db, data,
          :string_columns => string_columns,
          :schema => db.schema(table_name),
          :table => table_name
        )
      end

      def complete?
        return true if state[:partition_range].nil?
        min_pk, max_pk = state[:partition_range]
        # Complete when we've fetched past the max PK
        state[:last_pk] && state[:last_pk] >= max_pk
      end
    end
  end
end
