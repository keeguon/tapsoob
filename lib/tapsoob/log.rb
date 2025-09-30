# -*- encoding : utf-8 -*-
module Tapsoob
  def self.log=(log)
    @@log = log
  end

  def self.log
    @@log ||= begin
      require 'logger'
      log = Logger.new(STDERR)
      log.level = Logger::INFO
      log.datetime_format = "%Y-%m-%d %H:%M:%S"
      log
    end
  end
end
