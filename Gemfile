source "http://rubygems.org"

# monkey patching to support dual booting
module Bundler::SharedHelpers
  def default_lockfile=(path)
    @default_lockfile = path
  end
  def default_lockfile
    @default_lockfile ||= Pathname.new("#{default_gemfile}.lock")
  end
end

module ::Kernel
  def jruby?
    !(RUBY_PLATFORM =~ /java/).nil?
  end
end

if jruby?
  Bundler::SharedHelpers.default_lockfile = Pathname.new("#{Bundler::SharedHelpers.default_gemfile}_jruby.lock")

  # Bundler::Dsl.evaluate already called with an incorrect lockfile ... fix it
  class Bundler::Dsl
    # A bit messy, this can be called multiple times by bundler, avoid blowing the stack
    unless self.method_defined? :to_definition_unpatched
      alias_method :to_definition_unpatched, :to_definition
    end
    def to_definition(bad_lockfile, unlock)
      to_definition_unpatched(Bundler::SharedHelpers.default_lockfile, unlock)
    end
  end
end

group :test do
  gem 'rspec', '~> 3.2.0'
  gem 'simplecov', '~> 0.9.2'
end
