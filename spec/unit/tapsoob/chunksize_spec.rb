require 'spec_helper'
require 'tapsoob/chunksize'

RSpec.describe Tapsoob::Chunksize do
  subject(:cs) { described_class.new(1000) }

  describe '#initialize' do
    it 'stores the initial chunksize' do
      expect(cs.to_i).to eq(1000)
    end

    it 'starts with zero retries' do
      expect(cs.retries).to eq(0)
    end

    it 'starts with zero idle_secs' do
      expect(cs.idle_secs).to eq(0.0)
    end
  end

  describe '#reset_chunksize' do
    context 'with 0 retries (first failure)' do
      it 'resets to 10' do
        expect(cs.reset_chunksize).to eq(10)
      end
    end

    context 'with 1 retry' do
      it 'resets to 10' do
        cs.retries = 1
        expect(cs.reset_chunksize).to eq(10)
      end
    end

    context 'with 2+ retries' do
      it 'resets to 1' do
        cs.retries = 2
        expect(cs.reset_chunksize).to eq(1)
      end
    end
  end

  describe '#diff' do
    before do
      cs.start_time = 0.0
      cs.end_time   = 10.0
      cs.time_in_db = 3.0
      cs.idle_secs  = 2.0
    end

    it 'returns end_time - start_time - time_in_db - idle_secs' do
      expect(cs.diff).to eq(5.0)
    end
  end

  describe '#calc_new_chunksize' do
    def make(chunksize, diff_val)
      c = described_class.new(chunksize)
      # manufacture a diff by setting times such that diff == diff_val
      c.start_time = 0.0
      c.end_time   = diff_val
      c.time_in_db = 0.0
      c.idle_secs  = 0.0
      c
    end

    it 'halves (roughly) when diff > 3.0' do
      c = make(900, 3.5)
      expect(c.calc_new_chunksize).to eq((900 / 3.0).ceil)
    end

    it 'decrements by 100 when diff is 1.1..3.0' do
      c = make(900, 2.0)
      expect(c.calc_new_chunksize).to eq(800)
    end

    it 'doubles when diff < 0.8' do
      c = make(500, 0.5)
      expect(c.calc_new_chunksize).to eq(1000)
    end

    it 'increments by 100 when diff is 0.8..1.1' do
      c = make(500, 0.9)
      expect(c.calc_new_chunksize).to eq(600)
    end

    it 'never returns less than 1' do
      c = make(1, 5.0)
      expect(c.calc_new_chunksize).to be >= 1
    end

    it 'holds chunksize unchanged when retries > 0' do
      c = make(500, 0.5)
      c.retries = 1
      expect(c.calc_new_chunksize).to eq(500)
    end
  end

  describe '#time_delta' do
    it 'returns elapsed seconds for the block' do
      delta = cs.time_delta { sleep 0.01 }
      expect(delta).to be_between(0.005, 1.0)
    end
  end
end
