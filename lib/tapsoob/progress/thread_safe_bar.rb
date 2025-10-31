# -*- encoding : utf-8 -*-

module Tapsoob
  module Progress
    # Thread-safe progress bar that reports to a MultiBar
    class ThreadSafeBar < Bar
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
        # Get dynamic title width from MultiBar for consistent alignment
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

      # Override clear to do nothing (managed by MultiBar)
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
  end
end
