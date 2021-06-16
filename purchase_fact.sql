-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."receipt_oc" ADD COLUMN "is_metabase_purchase_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_purchase_fact()
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        purchase_fact_source RECORD;
        last_id INT;
    BEGIN
        FOR purchase_fact_source IN (SELECT * FROM dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'SELECT id, nyuka_sku_code, nyuka_ten_code, nohn_bi, den_no, qty FROM receipt_oc WHERE is_metabase_purchase_sync=false ORDER BY id ASC') AS rows(id int, nyuka_sku_code varchar, nyuka_ten_code varchar, nohn_bi date, den_no varchar, qty float)) LOOP
            INSERT INTO purchase_fact (
                product_code,
                store_code,
                date_key,
                purchase_number,
                total_qty
            ) VALUES (
                purchase_fact_source.nyuka_sku_code,
                purchase_fact_source.nyuka_ten_code,
                TO_CHAR(purchase_fact_source.nohn_bi, 'yyyymmdd'),
                purchase_fact_source.den_no,
                purchase_fact_source.qty
            );

            last_id = purchase_fact_source.id;
        END LOOP;

        -- update is_metabase_purchase_sync = true
        PERFORM  dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'UPDATE receipt_oc SET is_metabase_purchase_sync = true WHERE is_metabase_purchase_sync=false AND id <=' || last_id);     
    END;
$function$