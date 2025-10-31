# -*- encoding : utf-8 -*-

module Tapsoob
  module Progress
    # MultiBar manages multiple progress bars in parallel with a clean interface:
    # - N progress bar lines (constantly updating)
    # - 1 separator line
    # - 1 info message line (shows latest INFO, gets replaced)
    class MultiBar
      def initialize(max_bars = 4)
        @max_bars = max_bars
        @bars = []
        @mutex = Mutex.new
        @active = true
        @out = STDOUT
        @last_update = Time.now
        @max_title_width = 14  # Minimum width, will grow with longer titles
        @initialized = false
        @total_lines = 0  # Total lines: max_bars + separator + info line
        @info_message = ""  # Current info message to display
        @start_time = Time.now  # Track total elapsed time
        @terminal_width = get_terminal_width
      end

      # Get terminal width, default to 160 if can't detect
      def get_terminal_width
        require 'io/console'
        IO.console&.winsize&.[](1) || 160
      rescue
        160
      end

      # Create a new progress bar and return it
      def create_bar(title, total)
        @mutex.synchronize do
          # Initialize display area on first bar creation
          unless @initialized
            @total_lines = @max_bars + 2  # bars + separator + info line
            @total_lines.times { @out.print "\n" }
            @out.flush
            @initialized = true
          end

          # Remove any existing bar with the same title to prevent duplicates
          @bars.reject! { |b| b.title == title }

          # Update max title width to accommodate longer titles
          @max_title_width = [@max_title_width, title.length].max

          bar = ThreadSafeBar.new(title, total, self)
          @bars << bar
          bar
        end
      end

      # Update the info message line (called from outside for INFO logs)
      def set_info(message)
        @mutex.synchronize do
          return unless @active
          @info_message = message
          redraw_all if @initialized
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

      # Stop all progress bars and clear them from display
      def stop
        @mutex.synchronize do
          return unless @active  # Already stopped
          @active = false

          # Clear all lines (progress bars + separator + info line)
          if @total_lines > 0 && @initialized
            # Move cursor up to first line
            @out.print "\e[#{@total_lines}A"

            # Clear each line
            @total_lines.times do
              @out.print "\r\e[2K\n"
            end

            # Move cursor back to start
            @out.print "\e[#{@total_lines}A\r"
          end

          @out.flush
        end
      end

      private

      # Check if enough time has passed to redraw (throttle to 10 updates/sec)
      def should_redraw?
        Time.now - @last_update >= 0.1
      end

      def redraw_all(force = false)
        return unless @active
        return if @bars.empty?

        render_active_display
      end

      # Format elapsed time as HH:MM:SS
      def format_elapsed_time
        elapsed = Time.now - @start_time
        hours = (elapsed / 3600).to_i
        minutes = ((elapsed % 3600) / 60).to_i
        seconds = (elapsed % 60).to_i
        sprintf("%02d:%02d:%02d", hours, minutes, seconds)
      end

      # Render the complete display: progress bars + separator + info line
      def render_active_display
        return if @total_lines == 0

        # Show the last N bars (finished or not) - creates a rolling window effect
        # As new tables start, old completed ones scroll off the top
        bars_to_draw = @bars.last(@max_bars)

        # Move cursor up to first line
        @out.print "\e[#{@total_lines}A"

        # Draw progress bars (they handle their own width)
        @max_bars.times do |i|
          @out.print "\r\e[K"
          bars_to_draw[i].render_to(@out) if i < bars_to_draw.length
          @out.print "\n"
        end

        # Draw separator line using box drawing character
        @out.print "\r\e[K"
        @out.print "─" * @terminal_width
        @out.print "\n"

        # Draw info message line with elapsed time on the right
        @out.print "\r\e[K"
        unless @info_message.empty?
          elapsed_str = "Elapsed: #{format_elapsed_time}"
          # Calculate space to right-align elapsed time
          available_width = @terminal_width - @info_message.length - elapsed_str.length - 2
          if available_width > 0
            @out.print @info_message
            @out.print " " * available_width
            @out.print elapsed_str
          else
            # If too long, just show message
            @out.print @info_message[0...(@terminal_width - elapsed_str.length - 2)]
            @out.print "  " + elapsed_str
          end
        end
        @out.print "\n"

        @out.flush
      end
    end
  end
end

# Backward compatibility alias
MultiProgressBar = Tapsoob::Progress::MultiBar
