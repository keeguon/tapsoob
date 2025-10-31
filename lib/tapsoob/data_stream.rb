# -*- encoding : utf-8 -*-

module Tapsoob
  module DataStream
    # Require all DataStream classes
    require 'tapsoob/data_stream/base'
    require 'tapsoob/data_stream/keyed'
    require 'tapsoob/data_stream/keyed_partition'
    require 'tapsoob/data_stream/interleaved'
    require 'tapsoob/data_stream/file_partition'
  end
end
