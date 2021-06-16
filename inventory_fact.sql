-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."teraoka_sales_temp" ADD COLUMN "is_metabase_inventory_sync" bool DEFAULT FALSE;
ALTER TABLE "public"."receipt_oc" ADD COLUMN "is_metabase_inventory_sync" bool DEFAULT FALSE;
ALTER TABLE "public"."disposal" ADD COLUMN "is_metabase_inventory_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_inventory_fact()
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        inventory_fact_source RECORD;
        last_id INT;
    BEGIN
        -- select from teraoka_sales_temp
        FOR inventory_fact_source IN (SELECT * FROM dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'SELECT id, product_code, store_code, business_day_format, qty FROM teraoka_sales_temp WHERE is_metabase_inventory_sync=false ORDER BY id ASC') AS rows(id int, product_code varchar, store_code varchar, business_day_format date, qty float)) LOOP
            INSERT INTO inventory_fact (
                product_code,
                store_code,
                date_key,
                total_qty
            ) VALUES (
                inventory_fact_source.product_code,
                inventory_fact_source.store_code,
                TO_CHAR(inventory_fact_source.business_day_format, 'yyyymmdd'),
                (-1 * inventory_fact_source.qty)
            );

            last_id = inventory_fact_source.id;
        END LOOP;

        -- update is_metabase_inventory_sync = true for teraoka_sales_temp
        PERFORM  dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'UPDATE teraoka_sales_temp SET is_metabase_inventory_sync = true WHERE is_metabase_inventory_sync=false AND id <=' || last_id);     
    

        -- select from receipt_oc
        FOR inventory_fact_source IN (SELECT * FROM dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'SELECT id, nyuka_sku_code, nyuka_ten_code, nohn_bi, qty FROM receipt_oc WHERE is_metabase_inventory_sync=false ORDER BY id ASC') AS rows(id int, nyuka_sku_code varchar, nyuka_ten_code varchar, nohn_bi date, qty float)) LOOP
            INSERT INTO inventory_fact (
                product_code,
                store_code,
                date_key,
                total_qty
            ) VALUES (
                inventory_fact_source.nyuka_sku_code,
                inventory_fact_source.nyuka_ten_code,
                TO_CHAR(inventory_fact_source.nohn_bi, 'yyyymmdd'),
                inventory_fact_source.qty
            );

            last_id = inventory_fact_source.id;
        END LOOP;

        -- update is_metabase_inventory_sync = true for receipt_oc
        PERFORM  dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'UPDATE receipt_oc SET is_metabase_inventory_sync = true WHERE is_metabase_inventory_sync=false AND id <=' || last_id);
        

        -- select from disposal
        FOR inventory_fact_source IN (SELECT * FROM dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'SELECT id, product_code, store_code, registration_date, waste_qty FROM disposal WHERE is_metabase_inventory_sync=false ORDER BY id ASC') AS rows(id int, product_code varchar, store_code varchar, registration_date date, waste_qty float)) LOOP
            INSERT INTO inventory_fact (
                product_code,
                store_code,
                date_key,
                total_qty
            ) VALUES (
                inventory_fact_source.product_code,
                inventory_fact_source.store_code,
                TO_CHAR(inventory_fact_source.registration_date, 'yyyymmdd'),
                (-1 * inventory_fact_source.waste_qty)
            );

            last_id = inventory_fact_source.id;
        END LOOP;

        -- update is_metabase_inventory_sync = true for receipt_oc
        PERFORM  dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'UPDATE disposal SET is_metabase_inventory_sync = true WHERE is_metabase_inventory_sync=false AND id <=' || last_id);
    END;
$function$