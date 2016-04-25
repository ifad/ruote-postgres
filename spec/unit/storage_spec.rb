require 'spec_helper'

describe Ruote::Postgres::Storage do
  let(:pg)         { db_connect }
  let(:table_name) { "documents" }

  before do
    Ruote::Postgres.create_table(pg, true, table_name)
  end

  describe "#initialize" do
    before do
      allow_any_instance_of(Ruote::Postgres::Storage).to receive(:replace_engine_configuration)
    end

    it "prepares the storage" do
      expect_any_instance_of(Ruote::Postgres::Storage).to receive(:replace_engine_configuration)
      Ruote::Postgres::Storage.new(pg)
    end

    it "prepares the storage with options" do
      options = {a: :b}

      expect_any_instance_of(Ruote::Postgres::Storage).to receive(:replace_engine_configuration).with(options)
      Ruote::Postgres::Storage.new(pg, options)
    end

    it "assigns the db connection" do
      storage = Ruote::Postgres::Storage.new(pg)
      expect(storage.instance_variable_get(:@pg)).to eq(pg)
    end

    it "assigns the table with default value" do
      storage = Ruote::Postgres::Storage.new(pg)
      expect(storage.instance_variable_get(:@table)).to eq(:documents)
    end

    it "assigns the table name" do
      options = {'pg_table_name' => "some_table_name"}

      storage = Ruote::Postgres::Storage.new(pg, options)
      expect(storage.instance_variable_get(:@table)).to eq(:some_table_name)
    end
  end

  describe "interface" do
    subject { Ruote::Postgres::Storage.new(pg) }

    describe "#put" do
      context "with new document" do
        it "works" do
          subject.put({ "type" => "expressions", "_id" => "1" })
          expect(pg.query("select * from documents").values).not_to be_empty
        end
      end

      it "returns true if the document has been deleted from the store" do
        expect(subject.put({"type" => "expressions", "_id" => "4", "_rev" => 1})).to be_truthy
      end

      it "returns a document if the revision has changed" do
        doc = insert(pg, table_name, typ: 'expressions', ide: '4', wfid: "ef5678", rev: 2, doc: {"e" => "f"})

        expect(subject.put({"type" => "expressions", "_id" => "4", "_rev" => 1})).to eq(doc)
      end

      it "returns nil when the document is successfully stored" do
        insert(pg, table_name, typ: 'expressions', ide: '4', wfid: "ef5678", rev: 1, doc: {"e" => "f"})

        expect(subject.put({"type" => "expressions", "_id" => "4", "_rev" => 1})).to be_nil
      end

      it "is thread safe" do
        threads = 4.times.map do |i|
          Thread.new do
            subject.put({ "type" => "expressions", "_id" => i.to_s })
          end
        end

        threads.each(&:join)
        expect(count(pg, table_name, "WHERE typ='expressions'")).to eq(4)
      end
    end

    describe "#get" do
      it "returns the document that matches the given type ans key" do
        doc = insert(pg, table_name, typ: 'msgs', ide: '1')
        expect(subject.get('msgs', '1')).to eq(doc)
      end

      it "is thread safe" do
        doc = insert(pg, table_name, typ: 'msgs', ide: '1')

        threads = 4.times.map do
          Thread.new do
            expect(subject.get('msgs', '1')).to eq(doc)
          end
        end

        threads.each(&:join)
      end
    end

    describe "#delete" do
      before do
        insert(pg, table_name, typ: 'msgs', ide: '1', wfid: "ab12", doc: {"h" => "h"})
        insert(pg, table_name, typ: 'expressions', ide: '2', wfid: "cd34")
        insert(pg, table_name, typ: 'expressions', ide: '3', wfid: "ef56", doc: {"c" => "d"})
      end

      it "returns true if already deleted" do
        expect(subject.delete({"type" => "expressions", "_id" => "4", "_rev" => 1})).to be_truthy
      end

      it "returns a document if the revision of the given document doesn't match the version of the stored document" do
        doc = insert(pg, table_name, typ: 'expressions', ide: '4', wfid: "ef5678", rev: 2, doc: {"e" => "f"})

        expect(subject.delete({"type" => "expressions", "_id" => "4", "_rev" => 1})).to eq(doc)
      end

      it "returns nil when successfully removed" do
        expect(subject.delete({"type" => "expressions", "_id" => "3", "_rev" => 1})).to be_nil
      end

      it "is thread safe" do
        threads = 4.times.map do |i|
          Thread.new do
            if i == 0
              expect(subject.delete({"type" => "expressions", "_id" => "3", "_rev" => 1})).to be_nil
            else
              expect(subject.delete({"type" => "expressions", "_id" => "4", "_rev" => 1})).to be_truthy
            end
          end
        end

        threads.each(&:join)
      end
    end

    describe "#get_many" do
      before do
        insert(pg, table_name, typ: 'msgs', ide: '1', wfid: "ab12", doc: {"h" => "h"})
        insert(pg, table_name, typ: 'expressions', ide: '2', wfid: "cd34")
        insert(pg, table_name, typ: 'expressions', ide: '3', wfid: "ef56", doc: {"c" => "d"})
      end

      describe "without opts" do
        it "return an array of matching documents whitout any opts" do
          expect(subject.get_many("expressions")).to match_array([{"type" => "expressions", "_id" => "3", "_rev" => 1, "c" => "d"},
                                                     {"type" => "expressions", "_id" => "2", "_rev" => 1, "a" => "b"}])
        end

        it "is thread safe" do
          threads = 4.times.map do
            Thread.new do
              expect(subject.get_many("expressions")).to match_array([{"type" => "expressions", "_id" => "3", "_rev" => 1, "c" => "d"},
                                                     {"type" => "expressions", "_id" => "2", "_rev" => 1, "a" => "b"}])
            end
          end
          threads.each(&:join)
        end
      end

      describe "with opts" do
        describe ":count" do
          it "returns the number of matching documents without explicit key as an integer" do
            expect(subject.get_many("expressions", nil, count: true)).to eq(2)
          end

          it "returns the number of matching documents with a matching key as an integer" do
            expect(subject.get_many("expressions", "cd34", count: true)).to eq(1)
          end
        end

        describe ":descending" do
          it "returns the list sorted by ide descending" do
            expect(subject.get_many("expressions", nil, descending: true)).to match_array([{"type" => "expressions", "_id" => "3", "_rev" => 1, "c" => "d"},
                                                                              {"type" => "expressions", "_id" => "2", "_rev" => 1, "a" => "b"}])
          end

          it "returns the list sorted by ide descending with an explicit key" do
            expect(subject.get_many("expressions", "cd34", descending: true)).to match_array([{"type" => "expressions", "_id" => "2", "_rev" => 1, "a" => "b"}])
          end
        end

        describe ":skip" do
          it "returns the list skiping some results" do
            expect(subject.get_many("expressions", nil, skip: 1)).to match_array([{"type" => "expressions", "_id" => "3", "_rev" => 1, "c" => "d"}])
          end

          it "returns the list skiping some reslts with an explicit key" do
            expect(subject.get_many("expressions", "cd34", skip: 1)).to match_array([])
          end
        end

        describe ":limit" do
          it "returns the list limiting some results" do
            expect(subject.get_many("expressions", nil, limit: 1)).to match_array([{"type" => "expressions", "_id" => "2", "_rev" => 1, "a" => "b"}])
          end

          it "returns the list limiting some reslts with an explicit key" do
            expect(subject.get_many("expressions", "cd34", limit: 1)).to match_array([{"type" => "expressions", "_id" => "2", "_rev" => 1, "a" => "b"}])
          end
        end
      end
    end

    describe "#ids" do
      before do
        insert(pg, table_name, typ: 'msgs', ide: '1')
        insert(pg, table_name, typ: 'expressions', ide: '2')
        insert(pg, table_name, typ: 'expressions', ide: '3')
      end

      it "returns a list of id's for the matching documents" do
        expect(subject.ids("expressions")).to match_array([ '2', '3' ])
      end

      it "is thread safe" do
        threads = 4.times.map do
          Thread.new do
            expect(subject.ids("expressions")).to match_array([ '2', '3' ])
          end
        end
        threads.each(&:join)
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

        expect(count(pg, table_name)).to eq(0)
      end

      it "is thread safe" do
        threads = 4.times.map do
          Thread.new do
            subject.clear
          end
        end
        threads.each(&:join)

        expect(count(pg, table_name)).to eq(0)
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

        expect(count(pg, table_name)).to eq(0)
      end

      it "is thread safe" do
        threads = 4.times.map do
          Thread.new do
            subject.purge!
          end
        end
        threads.each(&:join)

        expect(count(pg, table_name)).to eq(0)
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

        expect(count(pg, table_name, "WHERE typ='expressions'")).to eq(0)
      end

      it "is thread safe" do
        threads = 4.times.map do
          Thread.new do
            subject.purge_type!("expressions")
          end
        end
        threads.each(&:join)

        expect(count(pg, table_name, "WHERE typ='expressions'")).to eq(0)
      end
    end
  end
end
