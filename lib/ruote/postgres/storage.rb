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
                  doc text NOT NULL,
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

  #
  # A Postgres storage implementation for ruote >= 2.2.0.
  #
  #   require 'rubygems'
  #   require 'json' # gem install json
  #   require 'ruote'
  #   require 'ruote-postgres' # gem install ruote-postgres
  #
  #   sequel = PG.connect(dbname: 'ruote_test', user: 'ruote', password: 'ruote')
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

    # The underlying Sequel::Database instance
    #
    attr_reader :pg

    def initialize(pg, options={})

      @pg    = pg
      @table = (options['pg_table_name'] || :documents).to_sym

      replace_engine_configuration(options)
    end

    def put_msg(action, options)

      # put_msg is a unique action, no need for all the complexity of put

      do_insert(prepare_msg_doc(action, options), 1)

      nil
    end

    # Used to reserve 'msgs' and 'schedules'. Simply deletes the document,
    # return true if the delete was successful (ie if the reservation is
    # valid).
    #
    def reserve(doc)

      @pg.exec(%{DELETE FROM #{@table}
                 WHERE typ='#{doc['type']}' AND
                       ide='#{doc['_id']}' AND
                       rev=1
                 RETURNING *}).count > 0
    end

    def put_schedule(flavour, owner_fei, s, msg)

      # put_schedule is a unique action, no need for all the complexity of put

      doc = prepare_schedule_doc(flavour, owner_fei, s, msg)

      return nil unless doc

      do_insert(doc, 1)

      doc['_id']
    end

    def put(doc, opts={})

      cache_clear(doc)

      if doc['_rev']

        d = get(doc['type'], doc['_id'])

        return true unless d
        return d if d['_rev'].to_i != doc['_rev'].to_i
          # failures
      end

      nrev = doc['_rev'].to_i + 1

      begin

        do_insert(doc, nrev, opts[:update_rev])

      rescue ::PG::Error => de

        return (get(doc['type'], doc['_id']) || true)
          # failure
      end

      @pg.exec(%{DELETE FROM #{@table}
                 WHERE typ='#{doc['type']}' AND
                       ide='#{doc['_id']}' AND
                       rev<#{nrev}})

      nil
        # success
    end

    def get(type, key)

      cache_get(type, key) || do_get(type, key)
    end

    def delete(doc)

      raise ArgumentError.new('no _rev for doc') unless doc['_rev']

      cache_clear(doc)
        # usually not necessary, adding it not to forget it later on

      count = @pg.exec(%{DELETE FROM #{@table}
                         WHERE typ='#{doc['type']}' AND
                               ide='#{doc['_id']}' AND
                               rev<#{doc['_rev'].to_i}
                         RETURNING *}).count

      return (get(doc['type'], doc['_id']) || true) if count < 1
        # failure

      nil
        # success
    end

    def get_many(type, key=nil, opts={})

      cached = cache_get_many(type, key, opts)
      return cached if cached

      ds = " FROM #{@table} WHERE typ='#{type}' "

      keys = key ? Array(key) : nil

      ds += " AND wfid in ('#{keys.join("','")}') " if keys && keys.first.is_a?(String)

      return @pg.exec("SELECT count(*) as count" + ds)[0]["count"].to_i if opts[:count]

      ds += " ORDER BY ide #{opts[:descending] ? "DESC" : "ASC"}, rev DESC"
      ds += " LIMIT #{opts[:limit]} " if opts[:limit]
      ds += " OFFSET #{opts[:skip] || opts[:offset]} " if opts[:skip] || opts[:offset]

      docs = select_last_revs(@pg.exec("SELECT * " + ds))
      docs = docs.collect { |d| decode_doc(d) }

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

      @pg.exec("DELETE FROM #{@table}")
    end
    alias :clear :purge!

    # Calls #disconnect on the db. According to pg's doc, it closes
    # all the idle connections in the pool (not the active ones).
    #
    def shutdown

      @pg.close
    end

    # Grrr... I should sort the mess between close and shutdown...
    # Tests vs production :-(
    #
    def close

      @pg.close
    end

    # Mainly used by ruote's test/unit/ut_17_storage.rb
    #
    def add_type(type)

      # does nothing, types are differentiated by the 'typ' column
    end

    # Nukes a db type and reputs it (losing all the documents that were in it).
    #
    def purge_type!(type)

      @pg.exec("DELETE FROM #{@table} WHERE typ='#{type}'")
    end

    # A provision made for workitems, allow to query them directly by
    # participant name.
    #
    def by_participant(type, participant_name, opts={})

      raise NotImplementedError if type != 'workitems'

      docs = " FROM #{@table} WHERE typ='#{type}' AND participant_name='#{participant_name}' "

      return @pg.exec("SELECT count(*) as count" + docs)[0]["count"].to_i if opts[:count]

      docs += " ORDER BY ide ASC, rev DESC"
      docs += " LIMIT #{opts[:limit]} OFFSET #{opts[:offset] || opts[:skip]} "

      select_last_revs(docs).collect { |d| Ruote::Workitem.from_json(d["doc"]) }
    end

    # Querying workitems by field (warning, goes deep into the JSON structure)
    #
    def by_field(type, field, value, opts={})

      raise NotImplementedError if type != 'workitems'

      lk = [ '%"', field, '":' ]
      lk.push(Rufus::Json.encode(value)) if value
      lk.push('%')

      docs = " FROM #{@table} WHERE typ='#{type}' AND doc like '#{lk.join}' "

      return @pg.exec("SELECT count(*) as count" + docs)[0]["count"].to_i if opts[:count]

      docs += " ORDER BY ide ASC, rev DESC"
      docs += " LIMIT #{opts[:limit]} OFFSET #{opts[:offset] || opts[:skip]} "

      select_last_revs(docs).collect { |d| Ruote::Workitem.from_json(d["doc"]) }
    end

    def query_workitems(criteria)

      ds = " FROM #{@table} WHERE typ='workitems' "

      count = criteria.delete('count')

      limit = criteria.delete('limit')
      offset = criteria.delete('offset') || criteria.delete('skip')

      wfid =
        criteria.delete('wfid')
      pname =
        criteria.delete('participant_name') || criteria.delete('participant')

      ds += " AND ide like '%!#{wfid}' " if wfid
      ds += " AND participant_name='#{pname}' " if pname

      criteria.collect do |k, v|
        ds += " AND doc like '%\"#{k}\":#{Rufus::Json.encode(v)}%' "
      end

      return @pg.exec("SELECT count(*) as count" + ds)[0]["count"].to_i if count

      ds += " ORDER BY ide ASC, rev DESC LIMIT #{limit} OFFSET #{offset}"

      select_last_revs(ds).collect { |d| Ruote::Workitem.from_json(d["doc"]) }
    end

    # Used by the worker to indicate a new step begins. For ruote-sequel,
    # it means the cache can be prepared (a unique select yielding
    # all the info necessary for one worker step (expressions excluded)).
    #
    def begin_step

      prepare_cache
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

      # Use bound variables
      # http://sequel.rubyforge.org/rdoc/files/doc/prepared_statements_rdoc.html
      #
      # That makes Oracle happy (the doc field might > 4000 characters)
      #
      # Thanks Geoff Herney
      #
      @pg.exec(%{INSERT INTO #{@table}(ide, rev, typ, doc, wfid, participant_name)
                 VALUES('#{(doc['_id'])}',
                        #{(rev || 1)},
                        '#{(doc['type'])}',
                        '#{(Rufus::Json.encode(doc) || '')}',
                        '#{(extract_wfid(doc) || '')}',
                        '#{(doc['participant_name'] || '')}')})
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

    # Weed out older docs (same ide, smaller rev).
    #
    # This could all have been done via SQL, but those inconsistencies
    # are rare, the cost of the pumped SQL is not constant :-(
    #
    def select_last_revs(docs)
      a = []

      docs.collect{ |doc| a << doc if a.last.nil? || doc["ide"] != a.last["ide"] }

      a
    end

    #--
    # worker step cache
    #
    # in order to cut down the number of selects, do one select with
    # all the information the worker needs for one step of work
    #++

    CACHED_TYPES = %w[ msgs schedules configurations variables ]

    # One select to grab in all the info necessary for a worker step
    # (expressions excepted).
    #
    def prepare_cache

      CACHED_TYPES.each { |t| cache[t] = {} }

      @pg.exec(%{SELECT ide, typ, doc FROM #{@table}
                     WHERE typ in ('#{CACHED_TYPES.join("','")}')
                     ORDER BY ide ASC, rev DESC}).each do |d|
        (cache[d["typ"]] ||= {})[d["ide"]] ||= decode_doc(d)
      end

      cache['variables']['trackers'] ||=
        { '_id' => 'trackers', 'type' => 'variables', 'trackers' => {} }
    end

    # Ask the cache for a doc. Returns nil if it's not cached.
    #
    def cache_get(type, key)

      (cache[type] || {})[key]
    end

    # Ask the cache for a set of documents. Returns nil if it's not cached
    # or caching is not OK.
    #
    def cache_get_many(type, keys, options)

      if !options[:batch] && CACHED_TYPES.include?(type) && cache[type]
        cache[type].values
      else
        nil
      end
    end

    # Removes a document from the cache.
    #
    def cache_clear(doc)

      (cache[doc['type']] || {}).delete(doc['_id'])
    end

    # Returns the cache for the given thread. Returns {} if there is no
    # cache available.
    #
    def cache

      worker = Thread.current['ruote_worker']

      return {} unless worker

      (Thread.current["cache_#{worker.name}"] ||= {})
    end
  end
end
end
