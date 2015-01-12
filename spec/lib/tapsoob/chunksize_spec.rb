require 'spec_helper'
require 'tapsoob/chunksize'

describe Tapsoob::Chunksize do
  subject(:tapsoob) { Tapsoob::Chunksize.new(1) }
  let(:chunksize) { double('chunksize') }
  chunksize = Tapsoob::Chunksize.new(chunksize)
  describe '#new' do
    it 'works' do
      result = Tapsoob::Chunksize.new(chunksize)
      expect(result).not_to be_nil
    end

    describe '#initialize' do
      it { should respond_to :chunksize }
      it { should respond_to :idle_secs }
      it { should respond_to :retries }
    end
  end

  describe '#to_i' do

    it { expect(tapsoob.to_i).to eq(1) }
    it { expect(tapsoob.to_i).to be_a(Integer) }
    it 'works' do
      chunksize = Tapsoob::Chunksize.new(chunksize)
      result = chunksize.to_i
      expect(result).not_to be_nil
    end

    context 'converts to type integer' do
      it { expect(tapsoob.to_i).to eq(1) }
      it { expect(tapsoob.to_i).to be_an(Integer) }
    end
  end

  describe '#reset_chunksize' do

    context 'retries <= 1' do
      it { expect(tapsoob.retries).to eq(0) }
      it { expect(tapsoob.reset_chunksize).to eq(10) }
    end

    it 'works' do
      chunksize = Tapsoob::Chunksize.new(chunksize)
      result = chunksize.reset_chunksize
      expect(result).not_to be_nil
    end
  end


  describe '#diff' do
    it 'works' do
      chunksize = Tapsoob::Chunksize.new(chunksize)
      chunksize.start_time = 1
      chunksize.end_time = 10
      chunksize.time_in_db = 2
      chunksize.idle_secs = 3
      result = chunksize.diff
      expect(result).not_to be_nil
    end
  end

  describe '#time_in_db=' do
    it 'works' do
      chunksize = Tapsoob::Chunksize.new(chunksize)
      result = chunksize.time_in_db = (1)
      expect(result).not_to be_nil
    end
  end

  describe '#time_delta' do
    it 'works' do
      chunksize = double('chunksize')
      chunksize = Tapsoob::Chunksize.new(chunksize)
      result = chunksize.time_delta
      expect(result).not_to be_nil
    end
  end

  describe '#calc_new_chunksize' do
    it 'works' do
      chunksize = Tapsoob::Chunksize.new(1)
      chunksize.start_time = 1
      chunksize.end_time = 10
      chunksize.time_in_db = 2
      chunksize.idle_secs = 3
      result = chunksize.calc_new_chunksize
      expect(result).not_to be_nil
    end
  end
end
