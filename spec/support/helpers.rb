module Ruote
  module Postgres
    module Helpers
      def columns(pg, table)
        pg.exec(%{SELECT attname FROM pg_attribute, pg_type
                  WHERE typname = '#{table}'
                  AND attname NOT IN ('cmin', 'cmax', 'ctid', 'oid', 'tableoid', 'xmin', 'xmax')
                  AND attrelid = typrelid}).collect{ |row| row["attname"] }
      end

      def delete_table(pg, table)
        pg.exec("DROP TABLE IF EXISTS #{table}")
      end

      def create_table(pg, table)
        pg.exec("CREATE TABLE #{table} (ide character varying(255) NOT NULL)")
      end

      def insert(pg, table, data = { })
        doc = pg.exec(%{INSERT INTO #{table}(ide, rev, typ, doc, wfid, participant_name)
                        VALUES('#{(data[:ide])}',
                               #{(data[:rev] || 1)},
                               '#{(data[:typ])}',
                               '#{(data[:doc] || Rufus::Json.encode({a: :b}))}',
                               '#{(data[:wfid] || '')}',
                               '#{(data[:participant_name] || '')}')
                        RETURNING *})[0]["doc"]
        Rufus::Json.decode(doc)
      end

      def count(pg, table, where = '')
        pg.exec("SELECT count(*) as count FROM #{table} #{where}")[0]['count'].to_i
      end
    end
  end
end
