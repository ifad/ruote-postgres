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
    end
  end
end
