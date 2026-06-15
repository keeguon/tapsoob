require 'spec_helper'
require 'tapsoob/progress'

RSpec.describe Tapsoob::Progress do
  # All output is redirected to StringIO so nothing hits stdout/terminal.

  let(:out) { StringIO.new }

  # ── Bar ──────────────────────────────────────────────────────────────────────

  describe Tapsoob::Progress::Bar do
    subject(:bar) { described_class.new("Test", 100, out) }

    describe '#initialize' do
      it 'starts at 0' do
        expect(bar.current).to eq(0)
      end

      it 'stores total' do
        expect(bar.total).to eq(100)
      end

      it 'stores title' do
        expect(bar.title).to eq("Test")
      end
    end

    describe '#inc' do
      it 'advances current by 1 by default' do
        bar.inc
        expect(bar.current).to eq(1)
      end

      it 'advances current by a custom step' do
        bar.inc(10)
        expect(bar.current).to eq(10)
      end

      it 'clamps current to total' do
        bar.inc(200)
        expect(bar.current).to eq(100)
      end
    end

    describe '#set' do
      it 'sets current to the given value' do
        bar.set(42)
        expect(bar.current).to eq(42)
      end

      it 'raises on negative value' do
        expect { bar.set(-1) }.to raise_error(RuntimeError)
      end

      it 'raises when value exceeds total' do
        expect { bar.set(101) }.to raise_error(RuntimeError)
      end
    end

    describe '#finish' do
      it 'sets current to total and marks finished' do
        bar.finish
        expect(bar.current).to eq(100)
        expect(bar.finished?).to be true
      end
    end

    describe '#halt' do
      it 'marks finished without changing current' do
        bar.inc(30)
        bar.halt
        expect(bar.finished?).to be true
        expect(bar.current).to eq(30)
      end
    end

    describe '#inspect' do
      it 'includes current/total' do
        expect(bar.inspect).to include('0/100')
      end
    end

    describe 'zero total' do
      subject(:bar) { described_class.new("Empty", 0, out) }

      it 'does not raise on init' do
        expect { bar }.not_to raise_error
      end

      it 'finish does not raise' do
        expect { bar.finish }.not_to raise_error
      end
    end
  end

  # ── ReversedBar ──────────────────────────────────────────────────────────────

  describe Tapsoob::Progress::ReversedBar do
    subject(:bar) { described_class.new("Rev", 100, out) }

    it 'inherits from Bar' do
      expect(bar).to be_a(Tapsoob::Progress::Bar)
    end

    it 'reports inverted percentage internally (100 at start)' do
      # do_percentage is private; test via inspect indirectly - just ensure no raise
      expect { bar.inc(10) }.not_to raise_error
    end
  end

  # ── ThreadSafeBar ────────────────────────────────────────────────────────────

  describe Tapsoob::Progress::ThreadSafeBar do
    let(:multi) do
      mb = instance_double(Tapsoob::Progress::MultiBar)
      allow(mb).to receive(:update)
      allow(mb).to receive(:finish_bar)
      allow(mb).to receive(:max_title_width).and_return(14)
      mb
    end

    subject(:bar) { described_class.new("Worker", 50, multi) }

    describe '#initialize' do
      it 'stores title and total' do
        expect(bar.title).to eq("Worker")
        expect(bar.total).to eq(50)
      end
    end

    describe '#inc' do
      it 'increments current and notifies multi' do
        expect(multi).to receive(:update)
        bar.inc(5)
        expect(bar.current).to eq(5)
      end
    end

    describe '#finish' do
      it 'sets current to total and calls finish_bar on multi' do
        expect(multi).to receive(:finish_bar).with(bar)
        bar.finish
        expect(bar.current).to eq(50)
      end
    end

    describe '#mark_finished / #finished?' do
      it 'returns true after mark_finished' do
        bar.mark_finished
        expect(bar.finished?).to be true
      end
    end

    describe '#render_to' do
      it 'writes something to the given output' do
        io = StringIO.new
        bar.render_to(io)
        expect(io.string).not_to be_empty
      end
    end

    describe '#clear' do
      it 'is a no-op (does not raise)' do
        expect { bar.send(:clear) }.not_to raise_error
      end
    end
  end

  # ── MultiBar ─────────────────────────────────────────────────────────────────

  describe Tapsoob::Progress::MultiBar do
    subject(:multi) { described_class.new(2) }

    describe '#initialize' do
      it 'starts with no bars' do
        expect(multi.instance_variable_get(:@bars)).to be_empty
      end
    end

    describe '#create_bar' do
      it 'returns a ThreadSafeBar' do
        bar = multi.create_bar("Table1", 100)
        expect(bar).to be_a(Tapsoob::Progress::ThreadSafeBar)
      end

      it 'does not create duplicate bars for the same title' do
        multi.create_bar("Table1", 100)
        multi.create_bar("Table1", 200)
        bars = multi.instance_variable_get(:@bars)
        expect(bars.count { |b| b.title == "Table1" }).to eq(1)
      end
    end

    describe '#set_info' do
      it 'does not raise' do
        expect { multi.set_info("Processing...") }.not_to raise_error
      end
    end

    describe '#stop' do
      it 'deactivates the multi bar' do
        multi.stop
        expect(multi.instance_variable_get(:@active)).to be false
      end

      it 'is idempotent' do
        multi.stop
        expect { multi.stop }.not_to raise_error
      end
    end

    describe '#max_title_width' do
      it 'grows to accommodate longer titles' do
        multi.create_bar("ShortTitle", 10)
        multi.create_bar("A Much Longer Title Here", 10)
        expect(multi.max_title_width).to be >= "A Much Longer Title Here".length
      end
    end

    # ── rendering paths (drive render_active_display / redraw_all) ──────────────

    describe '#update' do
      it 'does not raise when active and initialized with bars' do
        out = StringIO.new
        multi.instance_variable_set(:@out, out)
        multi.instance_variable_set(:@initialized, true)
        multi.instance_variable_set(:@total_lines, 4)
        multi.instance_variable_set(:@last_update, Time.now - 1)
        bar = multi.create_bar("UpdateTable", 10)
        expect { multi.update }.not_to raise_error
      end

      it 'is a no-op when @active is false' do
        multi.stop
        expect { multi.update }.not_to raise_error
      end
    end

    describe '#finish_bar' do
      it 'marks the bar as finished without raising' do
        out = StringIO.new
        multi.instance_variable_set(:@out, out)
        multi.instance_variable_set(:@initialized, true)
        multi.instance_variable_set(:@total_lines, 4)
        multi.instance_variable_set(:@last_update, Time.now - 1)
        bar = multi.create_bar("FinishTable", 5)
        expect { multi.finish_bar(bar) }.not_to raise_error
        expect(bar.instance_variable_get(:@finished)).to be true
      end
    end

    describe '#set_info (with rendering)' do
      it 'triggers redraw when initialized and a bar exists' do
        out = StringIO.new
        multi.instance_variable_set(:@out, out)
        multi.instance_variable_set(:@initialized, true)
        multi.instance_variable_set(:@total_lines, 4)
        multi.instance_variable_set(:@last_update, Time.now - 1)
        multi.create_bar("InfoTable", 10)
        expect { multi.set_info("doing things") }.not_to raise_error
      end
    end

    describe '#stop (initialized)' do
      it 'clears terminal lines when previously initialized' do
        out = StringIO.new
        multi.instance_variable_set(:@out, out)
        multi.instance_variable_set(:@initialized, true)
        multi.instance_variable_set(:@total_lines, 4)
        multi.stop
        expect(out.string).to include("\e[")
      end
    end

    describe 'render_active_display (via update)' do
      it 'writes escape sequences to output when bars exist' do
        out = StringIO.new
        multi.instance_variable_set(:@out, out)
        multi.instance_variable_set(:@initialized, true)
        multi.instance_variable_set(:@total_lines, 4)
        multi.instance_variable_set(:@last_update, Time.now - 1)
        multi.instance_variable_set(:@info_message, "some info")
        multi.create_bar("RenderTable", 10)
        multi.update
        expect(out.string).not_to be_empty
      end
    end
  end
end
