# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "tapsoob/version"

Gem::Specification.new do |s|
  # Metadata
  s.name        = "tapsoob"
  s.version     = Tapsoob::VERSION.dup
  s.authors     = ["Félix Bellanger"]
  s.email       = "felix.bellanger@faveod.com"
  s.homepage    = "https://github.com/Keeguon/tapsoob"
  s.summary     = "Simple tool to import/export databases."
  s.description = "Simple tool to import/export databases inspired by taps but OOB, meaning databases are imported/exported from the filesystem."
  s.license     = "MIT"

  # Manifest
  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # Dependencies
  s.add_dependency "sequel", "~> 4.17.0"

  s.add_development_dependency "mysql",   "~> 2.9.1"
  s.add_development_dependency "mysql2",  "~> 0.3.11"
  s.add_development_dependency "pg",      "~> 0.14.1"
  s.add_development_dependency "sqlite3", "~> 1.3.7"
  s.add_development_dependency "simplecov", '~> 0.9.2'
end
