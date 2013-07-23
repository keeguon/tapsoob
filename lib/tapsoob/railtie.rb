require 'tapsoob'
require 'rails'

module Tapsoob
  class Railtie < Rails::Railtie
    rake_tasks do
      load "tasks/tapsoob.rake"
    end
  end
end