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
    subject { Ruote::Postgres::Storage.new(pg) }

    describe "#put" do
      before do
      end

      it "returns true if de document has been deleted from the store" do
      end

      it "returns a document if the revision has changed" do
      end

      it "returns nil when the document is successfully stored" do
      end

    end

    describe "#get" do
      it "returns the document that matches the given type ans key" do
        doc =  insert(pg, table_name, typ: 'msgs', ide: '1')
        get_doc = subject.get('msgs', '1')

        get_doc.should == doc
      end
    end

    describe "#delete" do
      before do
      end

      it "returns true if already deleted" do
      end

      it "returns a document if the revision of the given document doesn't match the version of the stored document" do
      end

      it "returns nil when successfully removed" do
      end
    end

    describe "#get_many" do
      before do
      end

      it "returns the number of matching documents as an integer" do
      end

      it "returns an array of matching documents" do
      end
    end

    describe "#ids" do
      before do
      end

      it "returns a list of id's for the matching documents" do
      end
    end

    describe "#clear" do
      before do
        insert(pg, table_name, typ: 'msgs', ide: '1')
        insert(pg, table_name, typ: 'errors', ide: '1')
        insert(pg, table_name, typ: 'expressions', ide: '1')
      end

      it "removes all the documents" do
        subject.clear

        count(pg, table_name).should == 0
      end
    end

    describe "#purge!" do
      before do
        insert(pg, table_name, typ: 'msgs', ide: '1')
        insert(pg, table_name, typ: 'errors', ide: '1')
        insert(pg, table_name, typ: 'expressions', ide: '1')
      end

      it "cleans the store" do
        subject.purge!

        count(pg, table_name).should == 0
      end
    end

    describe "#add_type" do
      before do
      end

      it "ads a new document type to the store" do
      end
    end

    describe "#purge_type!" do
      before do
        insert(pg, table_name, typ: 'msgs', ide: '1')
        insert(pg, table_name, typ: 'expressions', ide: '1')
        insert(pg, table_name, typ: 'expressions', ide: '2')
      end

      it "cleans the store for the given type" do
        subject.purge_type!("expressions")

        count(pg, table_name, "WHERE typ='expressions'").should == 0
      end
    end
  end


  describe "instance methods" do
    describe "#put_msg" do
    end

    describe "#reserve" do
    end

    describe "#put_schedule" do
    end
  end
end
