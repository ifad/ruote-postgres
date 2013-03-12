require 'rubygems'
require 'bundler/setup'
require 'yaml'
require 'ruote-postgres'

Dir[File.expand_path("../support/**/*.rb", __FILE__)].each {|f| require f}

RSpec.configure do |config|
  config.include Ruote::Postgres::Helpers
end
