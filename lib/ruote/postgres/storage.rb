# -*- coding: utf-8 -*-
#--
# Copyright (c) 2013-2013, Lleïr Borràs Metje, l.borrasmetje@ifad.ord
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Made in Roma.
#++

begin
  require 'yajl'
rescue LoadError => le
  require 'json'
end

require 'pg'
require 'rufus/json'
require 'ruote/storage/base'
require 'ruote/postgres/version'


module Ruote
module Postgres

  # Creates the 'documents' table necessary for this storage.
  #
  # If re_create is set to true, it will destroy any previous 'documents'
  # table and create it. If false (default) then the table will be created
  # if it doesn't already exist.
  #
  # It's also possible to change the default table_name from 'documents' to
  # something else with the optional third parameter
  #
  def self.create_table(pg, re_create=false, table_name='documents')

    table_exists = table_exists?(pg, table_name)

    pg.exec("DROP TABLE #{table_name}") if re_create && table_exists

    if !table_exists || re_create
      pg.exec(%{CREATE TABLE #{table_name} (
                  ide character varying(255) NOT NULL,
                  rev integer NOT NULL,
                  typ character varying(55) NOT NULL,
                  doc #{has_json?(pg) ? "json" : "text"} NOT NULL,
                  wfid character varying(255),
                  participant_name character varying(512))})

      pg.exec("CREATE INDEX #{table_name}_wfid_index ON #{table_name} USING btree (wfid)")
      pg.exec("ALTER TABLE ONLY #{table_name} ADD CONSTRAINT #{table_name}_pkey PRIMARY KEY (typ, ide, rev)")
    end

  end

  def self.table_exists?(pg, table_name)
    pg.exec(%{SELECT exists(SELECT relname
                            FROM pg_class
                            WHERE relname='#{table_name}' AND relkind='r')
                     as result;})[0]["result"] == "t"
  end

  def self.server_version(pg)
    pg.exec("show server_version")[0]["server_version"].split(".").map(&:to_i)
  end

  def self.has_json?(pg)
    version = server_version(pg)
    version[0] >= 9 && version[1] >= 2
  end

  #
  # A Postgres storage implementation for ruote >= 2.2.0.
  #
  #   require 'rubygems'
  #   require 'json' # gem install json
  #   require 'ruote'
  #   require 'ruote-postgres' # gem install ruote-postgres
  #
  #   opg = PG.connect(dbname: 'ruote_test', user: 'ruote', password: 'ruote')
  #
  #   opts = { 'remote_definition_allowed' => true }
  #
  #   engine = Ruote::Engine.new(
  #     Ruote::Worker.new(
  #       Ruote::Postgres::Storage.new(pg, opts)))
  #
  #   # ...
  #
  class Storage

    include Ruote::StorageBase

    # The underlying Postgres::Database instance
    #
    attr_reader :pg
    attr_reader :pg_channel

    def initialize(pg, options={})
      @pg         = pg
      @pg_channel = "ruote_postgres"
      @table      = (options['pg_table_name'] || :documents).to_sym

      replace_engine_configuration(options)
    end

    def put(doc, opts={})
      if doc['_rev']

        d = get(doc['type'], doc['_id'])

        return true unless d
        return d if d['_rev'].to_i != doc['_rev'].to_i # failures
      end

      nrev = doc['_rev'].to_i + 1

      begin

        do_insert(doc, nrev, opts[:update_rev])

      rescue ::PG::Error => de
        return (get(doc['type'], doc['_id']) || true) # failure
      end

      @pg.exec(%{DELETE FROM #{@table}
                 WHERE typ='#{doc['type']}' AND
                       ide='#{doc['_id']}' AND
                       rev<#{nrev}})

      notify('DELETE')

      nil # success
    end

    def get(type, key)
      do_get(type, key)
    end

    def delete(doc)
      raise ArgumentError.new('no _rev for doc') unless doc['_rev']

      count = @pg.exec(%{DELETE FROM #{@table}
                         WHERE typ='#{doc['type']}' AND
                               ide='#{doc['_id']}' AND
                               rev=#{doc['_rev'].to_i}
                         RETURNING *}).count

      return (get(doc['type'], doc['_id']) || true) if count < 1 # failure

      notify('DELETE')

      nil # success
    end

    def get_many(type, key=nil, opts={})
      ds = " FROM #{@table} WHERE typ='#{type}' "

      keys = key ? Array(key) : nil

      ds += " AND wfid in ('#{keys.join("','")}') " if keys && keys.first.is_a?(String)

      return @pg.exec("SELECT count(*) as count" + ds)[0]["count"].to_i if opts[:count]

      ds += " ORDER BY ide #{opts[:descending] ? "DESC" : "ASC"}, rev DESC"
      ds += " LIMIT #{opts[:limit]} " if opts[:limit]
      ds += " OFFSET #{opts[:skip] || opts[:offset]} " if opts[:skip] || opts[:offset]

      docs = @pg.exec("SELECT * " + ds).collect { |d| decode_doc(d) }

      if keys && keys.first.is_a?(Regexp)
        docs.select { |doc| keys.find { |key| key.match(doc['_id']) } }
      else
        docs
      end

      # (pass on the dataset.filter(:wfid => /regexp/) for now
      # since we have potentially multiple keys)
    end

    # Returns all the ids of the documents of a given type.
    #
    def ids(type)
      @pg.exec(%{SELECT DISTINCT(ide) FROM #{@table}
                 WHERE typ='#{type}'
                 ORDER BY ide}).collect{|row| row["ide"]}
    end

    # Nukes all the documents in this storage.
    #
    def purge!
      d = @pg.exec("DELETE FROM #{@table}")

      notify('DELETE')

      d
    end
    alias :clear :purge!

    # Calls #disconnect on the db. According to pg's doc, it closes
    # all the idle connections in the pool (not the active ones).
    #
    def shutdown
      #@pg.finish
    end

    # Grrr... I should sort the mess between close and shutdown...
    # Tests vs production :-(
    #
    def close
      #@pg.finish
	end

    # Mainly used by ruote's test/unit/ut_17_storage.rb
    #
    def add_type(type)
      # does nothing, types are differentiated by the 'typ' column
    end

    # Nukes a db type and reputs it (losing all the documents that were in it).
    #
    def purge_type!(type)
      d = @pg.exec("DELETE FROM #{@table} WHERE typ='#{type}'")

      notify('DELETE')

      d
    end

    def wait_for_notify(timeout = nil, &block)
      @listen ||= @pg.exec("LISTEN #{@pg_channel}")
      @pg.wait_for_notify(timeout, &block)
    end

    protected

      def decode_doc(doc)
        return nil if doc.nil?

        doc = doc["doc"]

        doc = doc.read if doc.respond_to?(:read)

        Rufus::Json.decode(doc)
      end

      def do_insert(doc, rev, update_rev=false)
        doc = doc.send(
          update_rev ? :merge! : :merge,
          { '_rev' => rev, 'put_at' => Ruote.now_to_utc_s })

        i = @pg.exec(%{INSERT INTO #{@table}(ide, rev, typ, doc, wfid, participant_name)
                            VALUES('#{(doc['_id'])}',
                                   #{(rev || 1)},
                                   '#{(doc['type'])}',
                                   '#{(Rufus::Json.encode(doc) || '')}',
                                   '#{(extract_wfid(doc) || '')}',
                                   '#{(doc['participant_name'] || '')}')})

        notify('INSERT')

        i
      end

      def extract_wfid(doc)
        doc['wfid'] || (doc['fei'] ? doc['fei']['wfid'] : nil)
      end

      def do_get(type, key)
        d = @pg.exec(%{SELECT doc FROM #{@table}
                       WHERE typ='#{type}' AND
                             ide='#{key}'
                       ORDER BY rev DESC
                       LIMIT 1 OFFSET 0})

        decode_doc(d[0]) if d.count > 0
      end

      def server_version
        @server_version ||= @pg.exec("show server_version")[0]["server_version"].split(".").map(&:to_i)
      end

      def has_json?
        server_version[0] >= 9 && server_version[1] >= 2
      end

      def notify msg
        @pg.exec("NOTIFY #{@pg_channel}, '#{Time.now.to_f.to_s}::#{msg}'")
      end
  end
end
end
