$:.unshift File.dirname(__FILE__)

# internal requires
require 'tapsoob/operation'

module Tapsoob
  require 'tapsoob/railtie' if defined?(Rails)
end
