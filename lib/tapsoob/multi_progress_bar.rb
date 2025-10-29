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
    @initialized = false
    @last_update = Time.now
  end

  # Create a new progress bar and return it
  def create_bar(title, total)
    @mutex.synchronize do
      bar = ThreadSafeProgressBar.new(title, total, self, @bars.length)
      @bars << bar

      # Reserve a line for this new bar
      @out.print "\n"
      @out.flush

      # Initialize space on first bar creation
      unless @initialized
        @initialized = true
      end

      bar
    end
  end

  # Called by individual bars when they update
  def update
    @mutex.synchronize do
      return unless @active

      # Throttle updates to avoid flickering (max 10 updates per second)
      now = Time.now
      return if now - @last_update < 0.1
      @last_update = now

      redraw_all
    end
  end

  # Finish a specific bar
  def finish_bar(bar)
    @mutex.synchronize do
      if @active
        @last_update = Time.now - 1  # Force immediate update
        redraw_all
      end
    end
  end

  # Stop all progress bars and keep them visible
  def stop
    @mutex.synchronize do
      @active = false
      # Final redraw to show completed state
      redraw_all_final
      # Move cursor past all bars
      @out.print "\n"
      @out.flush
    end
  end

  private

  def redraw_all
    return unless @active
    return if @bars.empty?

    # Move up to first bar (cursor is currently after last bar on line N+1)
    @out.print "\e[#{@bars.length}A" if @bars.length > 0

    # Redraw each bar on its own line
    @bars.each do |bar|
      @out.print "\r\e[K"  # Move to start of line and clear it
      bar.render_to(@out)
      @out.print "\n"  # Always move to next line (moves us down by 1)
    end

    # After drawing N bars and moving down N times, we're back at line N+1
    # No need to add extra lines - cursor is already in the right position
    @out.flush
  end

  def redraw_all_final
    return if @bars.empty?

    # Move up to first bar
    @out.print "\e[#{@bars.length}A" if @bars.length > 0

    # Draw final state of each bar
    @bars.each do |bar|
      @out.print "\r\e[K"  # Move to start of line and clear it
      bar.render_to(@out)
      @out.print "\n"
    end

    @out.flush
  end
end

# Thread-safe progress bar that reports to a MultiProgressBar
class ThreadSafeProgressBar < ProgressBar
  def initialize(title, total, multi_progress_bar, bar_index)
    @multi_progress_bar = multi_progress_bar
    @bar_index = bar_index
    @out = STDOUT  # Need this for get_width to work
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
    # Recalculate terminal width to handle resizes and use full width
    width = get_width
    # The bar gets the remaining space after: title (14) + " " + percentage (4) + " " + "|" + "|" + " " + stat (15)
    # That's approximately 37 characters, so bar gets width - 37
    @terminal_width = [width - 37, 20].max

    arguments = @format_arguments.map {|method|
      method = sprintf("fmt_%s", method)
      send(method)
    }
    line = sprintf(@format, *arguments)

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
