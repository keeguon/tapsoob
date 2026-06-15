require 'spec_helper'
require 'tapsoob/config'

RSpec.describe Tapsoob::Config do
  describe '.verify_database_url' do
    it 'connects and lists tables without error for a valid URL' do
      expect { described_class.verify_database_url(sqlite_memory_url) }.not_to raise_error
    end

    it 'uses self.database_url when no argument is given' do
      described_class.database_url = sqlite_memory_url
      expect { described_class.verify_database_url }.not_to raise_error
    end
  end
end

RSpec.describe Tapsoob do
  describe '.log' do
    it 'returns a Logger instance' do
      expect(Tapsoob.log).to be_a(Logger)
    end

    it 'returns the same instance on repeated calls' do
      expect(Tapsoob.log).to equal(Tapsoob.log)
    end

    it 'accepts a custom logger' do
      custom = Logger.new(StringIO.new)
      Tapsoob.log = custom
      expect(Tapsoob.log).to equal(custom)
      # Reset to default for other tests
      Tapsoob.log = nil
    end
  end
end
