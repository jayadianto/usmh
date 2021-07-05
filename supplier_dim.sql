-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."master_data_supplier" ADD COLUMN "is_metabase_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_supplier_dim(db_name character varying, db_port character varying, db_host character varying, db_user character varying, db_password character varying)
RETURNS void
LANGUAGE plpgsql
AS $function$
    BEGIN
        INSERT INTO supplier_dim (supplier_code, supplier_name)
        SELECT supplier_code, supplier_kanji_name
        FROM dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT id, supplier_code, supplier_kanji_name FROM master_data_supplier WHERE is_metabase_sync=false ORDER BY id ASC') AS rows(id int, supplier_code varchar, supplier_kanji_name varchar);
        
        -- update is_metabase_sync = true
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE master_data_supplier SET is_metabase_sync = true WHERE is_metabase_sync=false');     
    END;
$function$

-- PROSES ETL
SELECT get_supplier_dim('middleware', '5432', '127.0.0.1', 'postgres', 'postgres');