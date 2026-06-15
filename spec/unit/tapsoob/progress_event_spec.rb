require 'spec_helper'
require 'tapsoob/progress_event'

RSpec.describe Tapsoob::ProgressEvent do
  before do
    described_class.enabled = true
    # Reset throttle state between examples
    described_class.instance_variable_set(:@last_progress_time, {})
  end

  after { described_class.enabled = false }

  # ── enabled? ─────────────────────────────────────────────────────────────────

  describe '.enabled?' do
    it 'reflects the value set by .enabled=' do
      described_class.enabled = false
      expect(described_class.enabled?).to be false
      described_class.enabled = true
      expect(described_class.enabled?).to be true
    end
  end

  # ── emit ─────────────────────────────────────────────────────────────────────

  describe '.emit' do
    it 'writes a PROGRESS: JSON line to STDERR when enabled' do
      err = StringIO.new
      stub_const('STDERR', err)
      described_class.emit('test_event', foo: 'bar')
      output = err.string
      expect(output).to include('PROGRESS:')
      data = JSON.parse(output.sub('PROGRESS: ', ''))
      expect(data['event']).to eq('test_event')
      expect(data['foo']).to eq('bar')
    end

    it 'does nothing when disabled' do
      described_class.enabled = false
      err = StringIO.new
      stub_const('STDERR', err)
      described_class.emit('ignored')
      expect(err.string).to be_empty
    end
  end

  # ── throttle helpers ─────────────────────────────────────────────────────────

  describe '.should_emit_progress?' do
    it 'returns true on the first call for a table' do
      expect(described_class.should_emit_progress?(:my_table)).to be true
    end

    it 'returns false when called again immediately' do
      described_class.should_emit_progress?(:my_table)
      expect(described_class.should_emit_progress?(:my_table)).to be false
    end
  end

  describe '.clear_throttle' do
    it 'resets state so the next call returns true' do
      described_class.should_emit_progress?(:my_table)
      described_class.clear_throttle(:my_table)
      expect(described_class.should_emit_progress?(:my_table)).to be true
    end
  end

  # ── high-level event helpers ──────────────────────────────────────────────────

  {
    schema_start:     [3],
    schema_complete:  [3],
    data_start:       [3, 100],
    data_complete:    [3, 100],
    indexes_start:    [3],
    indexes_complete: [3],
    sequences_start:  [],
    sequences_complete: [],
  }.each do |method, args|
    describe ".#{method}" do
      it 'does not raise' do
        expect { described_class.send(method, *args) }.not_to raise_error
      end
    end
  end

  describe '.table_start' do
    it 'resets throttle and emits without error' do
      expect { described_class.table_start(:users, 500, workers: 2) }.not_to raise_error
    end
  end

  describe '.table_progress' do
    it 'emits when throttle allows' do
      described_class.clear_throttle(:users)
      err = StringIO.new
      stub_const('STDERR', err)
      described_class.table_progress(:users, 50, 200)
      expect(err.string).to include('table_progress')
    end

    it 'skips emission when throttled' do
      described_class.should_emit_progress?(:users)  # consume the token
      err = StringIO.new
      stub_const('STDERR', err)
      described_class.table_progress(:users, 50, 200)
      expect(err.string).to be_empty
    end

    it 'emits 0% when total is 0' do
      described_class.clear_throttle(:empty_table)
      err = StringIO.new
      stub_const('STDERR', err)
      described_class.table_progress(:empty_table, 0, 0)
      data = JSON.parse(err.string.sub('PROGRESS: ', '').strip)
      expect(data['percentage']).to eq(0)
    end
  end

  describe '.table_complete' do
    it 'clears throttle and emits without error' do
      expect { described_class.table_complete(:users, 500) }.not_to raise_error
    end
  end

  describe '.error' do
    it 'emits an error event with the message' do
      err = StringIO.new
      stub_const('STDERR', err)
      described_class.error('something broke', table: 'users')
      data = JSON.parse(err.string.sub('PROGRESS: ', '').strip)
      expect(data['event']).to eq('error')
      expect(data['message']).to eq('something broke')
    end
  end
end
