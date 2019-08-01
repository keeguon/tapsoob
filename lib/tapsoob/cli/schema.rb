require 'thor'
require 'sequel'

require_relative '../schema'

module Tapsoob
  module CLI
    class Schema < Thor
      desc "console DATABASE_URL", "Create an IRB REPL connected to a database"
      def console(database_url)
        $db = Sequel.connect(database_url)
        require 'ripl'
        Ripl.start binding: binding
      end

      desc "dump DATABASE_URL", "Dump a database using a database URL"
      def dump(database_url)
        puts Tapsoob::Schema.dump(database_url)
      end

      desc "dump_table DATABASE_URL TABLE", "Dump a table from a database using a database URL"
      def dump_table(database_url, table)
        puts Tapsoob::Schema.dump_table(database_url, table)
      end

      desc "indexes DATABASE_URL", "Dump indexes from a database using a database URL"
      def indexes(database_url)
        puts Tapsoob::Schema.indexes(database_url)
      end

      desc "indexes_individual DATABASE_URL", "Dump indexes per table individually using a database URL"
      def indexes_individual(database_url)
        puts Tapsoob::Schema.indexes_individual(database_url)
      end

      desc "reset_db_sequences DATABASE_URL", "Reset database sequences using a database URL"
      def reset_db_sequences(database_url)
        Tapsoob::Schema.reset_db_sequences(database_url)
      end

      desc "load DATABASE_URL [FILENAME]", "Load a database schema from a file to a database using a database URL"
      def load(database_url, filename = nil)
        schema = if filename && File.exists?(filename)
          File.read(filename)
        else
          STDIN.read
        end

        begin
          Tapsoob::Schema.load(database_url, schema)
        rescue Exception => e
          throw e
        end
      end

      desc "load_indexes DATABASE_URL [FILENAME]", "Load indexes from a file to a database using a database URL"
      def load_indexes(database_url, filename = nil)
        indexes = if filename && File.exists?(filename)
          File.read(filename)
        else
          STDIN.read
        end

        begin
          Tapsoob::Schema.load_indexes(database_url, indexes)
        rescue Exception => e
          throw e
        end
      end
    end
  end
end
