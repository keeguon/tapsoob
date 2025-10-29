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
    @last_update = Time.now
    @reserved_lines = 0  # Track how many lines we've actually reserved
    @max_title_width = 14  # Minimum width, will grow with longer titles
  end

  # Create a new progress bar and return it
  def create_bar(title, total)
    @mutex.synchronize do
      # Remove any existing bar with the same title to prevent duplicates
      @bars.reject! { |b| b.title == title }

      # Update max title width to accommodate longer titles
      @max_title_width = [@max_title_width, title.length].max

      bar = ThreadSafeProgressBar.new(title, total, self)

      # Reserve a line for this new bar during active updates
      # Cap at 2 * max_bars to show active workers + some recent finished bars
      if @reserved_lines < @max_bars * 2
        @out.print "\n"
        @out.flush
        @reserved_lines += 1
      end

      @bars << bar
      bar
    end
  end

  # Get the current maximum title width for alignment
  # Note: Always called from within synchronized methods, so no mutex needed
  def max_title_width
    @max_title_width
  end

  # Called by individual bars when they update
  def update
    @mutex.synchronize do
      return unless @active
      return unless should_redraw?

      @last_update = Time.now
      redraw_all
    end
  end

  # Finish a specific bar - mark it as completed
  def finish_bar(bar)
    @mutex.synchronize do
      return unless @active

      bar.mark_finished

      # Respect throttle when finishing to avoid spamming redraws
      if should_redraw?
        @last_update = Time.now
        redraw_all
      end
      # If throttled, the next regular update will show the finished state
    end
  end

  # Stop all progress bars and keep them visible
  def stop
    @mutex.synchronize do
      @active = false

      # Final cleanup: remove any duplicate titles (keep the last occurrence of each unique title)
      @bars = @bars.reverse.uniq { |bar| bar.title }.reverse

      # Final redraw to show completed state (skip active check)
      redraw_all(true)
      # Move cursor past all bars
      @out.print "\n"
      @out.flush
    end
  end

  private

  # Check if enough time has passed to redraw (throttle to 10 updates/sec)
  def should_redraw?
    Time.now - @last_update >= 0.1
  end

  def redraw_all(force = false)
    return unless force || @active
    return if @bars.empty?

    if force && !@active
      render_final_display
    else
      render_active_display
    end
  end

  # Final display: show all completed bars
  def render_final_display
    # Clear the reserved lines first
    if @reserved_lines > 0
      @out.print "\r\e[#{@reserved_lines}A"
      @reserved_lines.times { @out.print "\r\e[K\n" }
    end

    # Print all bars (adds new lines as needed)
    @bars.each do |bar|
      @out.print "\r\e[K"
      bar.render_to(@out)
      @out.print "\n"
    end

    @out.flush
  end

  # Normal operation: show active bars + recent finished in reserved space
  def render_active_display
    return if @reserved_lines == 0

    # Partition bars in a single pass for efficiency
    active_bars, finished_bars = @bars.partition { |b| !b.finished? }

    # Build display: active bars first, then recent finished to fill remaining space
    # Ensure we don't request negative count from .last()
    remaining_space = [@reserved_lines - active_bars.length, 0].max
    bars_to_draw = active_bars + finished_bars.last(remaining_space)

    # If we have more bars than reserved lines, show only the most recent
    bars_to_draw = bars_to_draw.last(@reserved_lines) if bars_to_draw.length > @reserved_lines

    # Move up and redraw in reserved space
    @out.print "\r\e[#{@reserved_lines}A"
    @reserved_lines.times do |i|
      @out.print "\r\e[K"
      bars_to_draw[i].render_to(@out) if i < bars_to_draw.length
      @out.print "\n"
    end

    @out.flush
  end
end

# Thread-safe progress bar that reports to a MultiProgressBar
class ThreadSafeProgressBar < ProgressBar
  attr_reader :title

  def initialize(title, total, multi_progress_bar)
    @multi_progress_bar = multi_progress_bar
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
    @format_arguments = [:title, :percentage, :bar, :stat]
  end

  # Override show to notify multi-progress instead of direct output
  def show
    @previous_time = ::Time.now  # Update to prevent time-based refresh spam
    @multi_progress_bar.update
  end

  # Render this bar to the given output stream
  def render_to(out)
    # Get dynamic title width from MultiProgressBar for consistent alignment
    # Store as instance variable so parent class fmt_* methods can use it
    @title_width = @multi_progress_bar.max_title_width

    # Recalculate terminal width to handle resizes and use full width
    width = get_width
    # Calculate bar width: total_width - fixed_elements - padding
    # Fixed: title(variable) + " "(1) + percentage(4) + " "(1) + "|"(1) + "|"(1) + " "(1) + timer(15) = title_width + 25
    # Padding: +3 for timer fluctuations and safety
    fixed_chars = @title_width + 28
    @terminal_width = [width - fixed_chars, 20].max

    # Build format string with dynamic title width
    format = "%-#{@title_width}s %3d%% %s %s"
    arguments = @format_arguments.map { |method| send("fmt_#{method}") }
    line = sprintf(format, *arguments)

    # Ensure line doesn't exceed terminal width to prevent wrapping
    # Leave 2 chars margin for safety
    line = line[0, width - 2] if line.length > width - 2

    out.print(line)
  end

  # Override clear to do nothing (managed by MultiProgressBar)
  def clear
    # no-op
  end

  # Mark this bar as finished (for tracking)
  def mark_finished
    @finished_p = true
  end

  # Override to use the same @finished_p flag
  def finished?
    @finished_p
  end

  # Override finish to notify multi-progress
  def finish
    @current = @total
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
