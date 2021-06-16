-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."master_data_supplier" ADD COLUMN "is_metabase_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_supplier_dim()
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        supplier_dim_source RECORD;
        last_id INT;
    BEGIN
        FOR supplier_dim_source IN (SELECT * FROM dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'SELECT id, supplier_code, supplier_kanji_name FROM master_data_supplier WHERE is_metabase_sync=false ORDER BY id ASC') AS rows(id int, supplier_code varchar, supplier_kanji_name varchar)) LOOP
            INSERT INTO supplier_dim (
                supplier_code,
                supplier_name
            ) VALUES (
                supplier_dim_source.supplier_code,
                supplier_dim_source.supplier_kanji_name
            );

            last_id = supplier_dim_source.id;
        END LOOP;

        -- update is_metabase_sync = true
        PERFORM  dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'UPDATE master_data_supplier SET is_metabase_sync = true WHERE is_metabase_sync=false AND id <=' || last_id);     
    END;
$function$