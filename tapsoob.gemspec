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
  s.add_dependency "ripl", "~> 0.7.1"
  s.add_dependency "sequel", "~> 5.59.0"
  s.add_dependency "thor", "~> 1.2.1"

  if (RUBY_PLATFORM =~ /java/).nil?
    s.add_development_dependency "mysql2",  "~> 0.4.10"
    s.add_development_dependency "pg",      "~> 0.21.0"
    s.add_development_dependency "sqlite3", "~> 1.3.11"
  else
    s.platform = 'java'

    s.add_dependency "jdbc-mysql",    "~> 5.1.44"
    s.add_dependency "jdbc-postgres", "~> 42.2.25"
    s.add_dependency "jdbc-sqlite3",  "~> 3.28.0"
  end
end
