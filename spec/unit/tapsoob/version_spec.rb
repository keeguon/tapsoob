require 'spec_helper'
require 'tapsoob/version'

RSpec.describe Tapsoob do
  it 'has a version string' do
    expect(Tapsoob::VERSION).to match(/\A\d+\.\d+\.\d+\z/)
  end
end
