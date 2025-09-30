# -*- encoding: utf-8 -*-
require "./lib/tapsoob/version" unless defined? Tapsoob::VERSION

Gem::Specification.new do |s|
  # Metadata
  s.name        = "tapsoob"
  s.version     = Tapsoob::VERSION.dup
  s.authors     = ["Félix Bellanger", "Michael Chrisco"]
  s.email       = "felix.bellanger@faveod.com"
  s.homepage    = "https://github.com/Keeguon/tapsoob"
  s.summary     = "Simple tool to import/export databases."
  s.description = "Simple tool to import/export databases inspired by taps but OOB, meaning databases are imported/exported from the filesystem."
  s.license     = "MIT"

  # Manifest
  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.bindir        = 'bin'
  s.require_paths = ["lib"]

  # Dependencies
  s.add_dependency "sequel", "~> 5.96.0"
  s.add_dependency "thor", "~> 1.4.0"

  if (RUBY_PLATFORM =~ /java/).nil?
    s.add_development_dependency "mysql2",  "~> 0.5.7"
    s.add_development_dependency "pg",      "~> 1.6.2"
    s.add_development_dependency "sqlite3", "~> 2.7.4"
  else
    s.platform = 'java'

    s.add_dependency "jdbc-mysql",    "~> 9.1.0.1"
    s.add_dependency "jdbc-postgres", "~> 42.6.0"
    s.add_dependency "jdbc-sqlite3",  "~> 3.46.1.1"
  end
end
