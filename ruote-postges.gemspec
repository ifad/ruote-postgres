# encoding: UTF-8

Gem::Specification.new do |s|

  s.name = 'ruote-postgres'

  s.version = File.read(
    File.expand_path('../lib/ruote/postgres/version.rb', __FILE__)
  ).match(/ VERSION *= *['"]([^'"]+)/)[1]

  s.platform = Gem::Platform::RUBY
  s.authors = [ 'Lleïr Borràs Metje' ]
  s.email = [ 'l.borrasmetje@ifad.org' ]
  s.homepage = 'http://mine.ifad.org/git/ruote-postgres'
  s.summary = 'postgres storage for ruote (a workflow engine)'
  s.description = %q{
postgres storage for ruote (a workflow engine)
  }

  s.files = Dir[
    'Rakefile',
    'lib/**/*.rb', 'spec/**/*.rb',
    '*.gemspec', '*.txt', '*.rdoc', '*.md'
  ]

  s.add_runtime_dependency 'ruote'

  s.add_dependency 'rake'
  s.add_dependency 'pg', '0.14.1'
  s.add_dependency 'yajl-ruby'
  s.add_dependency 'json'

  s.add_development_dependency 'debugger'

  s.require_path = 'lib'
end

