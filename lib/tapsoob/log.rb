# -*- encoding : utf-8 -*-
module Tapsoob
  def self.log=(log)
    @@log = log
  end

  def self.log
    @@log ||= begin
      require 'logger'
      log = Logger.new($stderr)
      log.level = Logger::ERROR
      log.datetime_format = "%Y-%m-%d %H:%M:%S"
      log
    end
  end
end
