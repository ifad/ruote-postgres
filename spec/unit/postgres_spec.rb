require 'spec_helper'

describe Ruote::Postgres do
  let(:pg)         { db_connect }
  let(:klass)      { Ruote::Postgres }
  let(:table_name) { "documents" }

  before do
    delete_table(pg, table_name)
  end

  describe "#create_table" do

    describe "with default arguments" do
      it "queries if the table exist" do
        klass.should_receive(:table_exists?).with(pg, table_name)
        klass.create_table(pg)
      end

      it "it don't create the table if already exist" do
        create_table(pg, table_name)
        klass.create_table(pg)

        columns(pg, table_name).should =~ %w{ ide }
      end

      it "it creates the table" do
        klass.create_table(pg)

        columns(pg, table_name).should =~ %w{ ide rev typ doc wfid participant_name }
      end
    end

    describe "with re_create set to true" do
      it "it removes the table if it already exist" do
        create_table(pg, table_name)
        klass.create_table(pg, true)

        columns(pg, table_name).should =~ %w{ ide rev typ doc wfid participant_name }
      end
    end

    describe "diferent table name" do
      let(:table_name) { "r_documents" }

      it "it removes the table if it already exist" do
        klass.create_table(pg, false, table_name)

        columns(pg, table_name).should =~ %w{ ide rev typ doc wfid participant_name }
      end
    end
  end

  describe "#table_exists?" do
    it "returns true if it exists" do
      create_table(pg, table_name)
      klass.table_exists?(pg, table_name).should be_true
    end

    it "returns false if it doesn't exist" do
      klass.table_exists?(pg, table_name).should be_false
    end
  end
end
