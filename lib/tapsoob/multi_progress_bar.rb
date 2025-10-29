# -*- encoding : utf-8 -*-
require 'tapsoob/progress_bar'

# MultiProgressBar manages multiple progress bars in parallel
# Each bar gets its own line in the terminal
class MultiProgressBar
  def initialize(max_bars = 4)
    @max_bars = max_bars
    @bars = []
    @mutex = Mutex.new
    @active = true
    @out = STDOUT
  end

  # Create a new progress bar and return it
  def create_bar(title, total)
    @mutex.synchronize do
      bar = ThreadSafeProgressBar.new(title, total, self)
      @bars << bar

      # Reserve space for this bar
      redraw_all

      bar
    end
  end

  # Called by individual bars when they update
  def update
    @mutex.synchronize do
      redraw_all if @active
    end
  end

  # Finish a specific bar
  def finish_bar(bar)
    @mutex.synchronize do
      redraw_all
    end
  end

  # Stop all progress bars
  def stop
    @mutex.synchronize do
      @active = false
      # Move cursor to bottom
      @out.print "\n" * @bars.count
    end
  end

  private

  def redraw_all
    return unless @active

    # Move cursor up to the first bar
    @out.print "\r" + ("\e[A" * [@bars.count - 1, 0].max) if @bars.count > 1

    @bars.each_with_index do |bar, index|
      @out.print "\r"
      bar.render_to(@out)
      @out.print "\n" unless index == @bars.count - 1
    end

    # Move cursor back to last line
    @out.flush
  end
end

# Thread-safe progress bar that reports to a MultiProgressBar
class ThreadSafeProgressBar < ProgressBar
  def initialize(title, total, multi_progress_bar)
    @multi_progress_bar = multi_progress_bar
    # Don't call parent initialize, we'll manage output ourselves
    @title = title
    @total = total
    @terminal_width = 80
    @bar_mark = "="
    @current = 0
    @previous = 0
    @finished_p = false
    @start_time = ::Time.now
    @previous_time = @start_time
    @title_width = 14
    @format = "%-#{@title_width}s %3d%% %s %s"
    @format_arguments = [:title, :percentage, :bar, :stat]
  end

  # Override show to notify multi-progress instead of direct output
  def show
    @multi_progress_bar.update
  end

  # Render this bar to the given output stream
  def render_to(out)
    arguments = @format_arguments.map {|method|
      method = sprintf("fmt_%s", method)
      send(method)
    }
    line = sprintf(@format, *arguments)

    # Truncate or pad to terminal width
    width = @terminal_width + @title_width + 20 # approximate
    if line.length >= width
      line = line[0, width - 1]
    end

    out.print(line)
  end

  # Override clear to do nothing (managed by MultiProgressBar)
  def clear
    # no-op
  end

  # Override finish to notify multi-progress
  def finish
    @current = @total
    @finished_p = true
    @multi_progress_bar.finish_bar(self)
  end

  # Override inc to check if we need to update
  def inc(step = 1)
    @current += step
    @current = @total if @current > @total
    show_if_needed
    @previous = @current
  end
end
