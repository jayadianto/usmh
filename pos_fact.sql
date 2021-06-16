-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."teraoka_sales_temp" ADD COLUMN "is_metabase_pos_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_pos_fact()
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        pos_fact_source RECORD;
        last_id INT;
    BEGIN
        FOR pos_fact_source IN (SELECT * FROM dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'SELECT id, product_code, store_code, business_day_format, qty FROM teraoka_sales_temp WHERE is_metabase_pos_sync=false ORDER BY id ASC') AS rows(id int, product_code varchar, store_code varchar, business_day_format date, qty float)) LOOP
            INSERT INTO pos_fact (
                product_code,
                store_code,
                date_key,
                total_qty,
                sales_amount
            ) VALUES (
                pos_fact_source.product_code,
                pos_fact_source.store_code,
                TO_CHAR(pos_fact_source.business_day_format, 'yyyymmdd'),
                pos_fact_source.qty,
                1000 -- dummy 
            );

            last_id = pos_fact_source.id;
        END LOOP;

        -- update is_metabase_pos_sync = true
        PERFORM  dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'UPDATE teraoka_sales_temp SET is_metabase_pos_sync = true WHERE is_metabase_pos_sync=false AND id <=' || last_id);     
    END;
$function$