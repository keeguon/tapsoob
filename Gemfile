source "http://rubygems.org"

# load the gem's dependencies
if (RUBY_PLATFORM =~ /java/).nil?
  gemspec name: 'tapsoob'
else
  gemspec name: 'tapsoob-java'
end


group :test do
  gem 'rspec', '~> 3.2.0'
  gem 'simplecov', '~> 0.9.2'
end
