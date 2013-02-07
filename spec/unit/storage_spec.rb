require 'spec_helper'

describe Ruote::Postgres::Storage do
  let(:pg)         { PG.connect(dbname: 'ruote_test', user: 'ruote', password: 'ruote') }
  let(:table_name) { "documents" }

  before do
    Ruote::Postgres.create_table(pg, true, table_name)
  end

  describe "#initialize" do
    before do
      Ruote::Postgres::Storage.any_instance.stub(:replace_engine_configuration)
    end

    it "prepares the storage" do
      Ruote::Postgres::Storage.any_instance.should_receive(:replace_engine_configuration)
      Ruote::Postgres::Storage.new(pg)
    end

    it "prepares the storage with options" do
      options = {a: :b}

      Ruote::Postgres::Storage.any_instance.should_receive(:replace_engine_configuration).with(options)
      Ruote::Postgres::Storage.new(pg, options)
    end

    it "assigns the db connection" do
      storage = Ruote::Postgres::Storage.new(pg)
      storage.instance_variable_get(:@pg).should == pg
    end

    it "assigns the table with default value" do
      storage = Ruote::Postgres::Storage.new(pg)
      storage.instance_variable_get(:@table).should == :documents
    end

    it "assigns the table name" do
      options = {'pg_table_name' => "some_table_name"}

      storage = Ruote::Postgres::Storage.new(pg, options)
      storage.instance_variable_get(:@table).should == :some_table_name
    end
  end

  describe "interface" do
    describe "#put" do
    end

    describe "#get" do
    end

    describe "#delete" do
    end

    describe "#get_many" do
    end

    describe "#ids" do
    end

    describe "#clear" do
    end

    describe "#purge!" do
    end

    describe "#add_type" do
    end

    describe "#purge_type!" do
    end
  end
end
