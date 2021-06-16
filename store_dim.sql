-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."master_data_product" ADD COLUMN "is_metabase_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_store_dim()
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        store_dim_source RECORD;
        last_id INT;
    BEGIN
        FOR store_dim_source IN (SELECT * FROM dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'SELECT id, server_name, store_code FROM odoo_server WHERE is_metabase_sync=false ORDER BY id ASC') AS rows(id int, server_name varchar, store_code varchar)) LOOP
            INSERT INTO store_dim (
                store_code,
                store_name
            ) VALUES (
                store_dim_source.store_code,
                store_dim_source.server_name
            );

            last_id = store_dim_source.id;
        END LOOP;

        -- update is_metabase_sync = true
        PERFORM  dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'UPDATE odoo_server SET is_metabase_sync = true WHERE is_metabase_sync=false AND id <=' || last_id);     
    END;
$function$