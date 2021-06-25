-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."teraoka_sales_temp" ADD COLUMN "is_metabase_inventory_sync" bool DEFAULT FALSE;
ALTER TABLE "public"."receipt_oc" ADD COLUMN "is_metabase_inventory_sync" bool DEFAULT FALSE;
ALTER TABLE "public"."disposal" ADD COLUMN "is_metabase_inventory_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_inventory_fact(db_name character varying, db_port character varying, db_host character varying, db_user character varying, db_password character varying)
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        inventory_fact_source RECORD;
        last_id INT;
    BEGIN
        -- select from teraoka_sales_temp
        FOR inventory_fact_source IN (SELECT * FROM dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT tst.id, mdp.product_categ, tst.store_code, tst.business_day_format, tst.qty FROM teraoka_sales_temp AS tst INNER JOIN master_data_product AS mdp ON mdp.product_code=tst.product_code WHERE tst.is_metabase_inventory_sync=false ORDER BY tst.id ASC') AS rows(id int, product_categ_code varchar, store_code varchar, business_day_format date, qty float)) LOOP
            INSERT INTO inventory_fact (
                product_categ_code,
                store_code,
                date_key,
                total_qty
            ) VALUES (
                inventory_fact_source.product_categ_code,
                inventory_fact_source.store_code,
                TO_CHAR(inventory_fact_source.business_day_format, 'yyyymmdd'),
                (-1 * inventory_fact_source.qty)
            );

            last_id = inventory_fact_source.id;
        END LOOP;

        -- update is_metabase_inventory_sync = true for teraoka_sales_temp
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE teraoka_sales_temp SET is_metabase_inventory_sync = true WHERE is_metabase_inventory_sync=false AND id <=' || last_id);     
    

        -- select from receipt_oc
        FOR inventory_fact_source IN (SELECT * FROM dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT ro.id, mdp.product_categ, ro.nyuka_ten_code, ro.nohn_bi, ro.qty FROM receipt_oc AS ro INNER JOIN master_data_product AS mdp ON mdp.product_code=ro.nyuka_sku_code WHERE ro.is_metabase_inventory_sync=false ORDER BY ro.id ASC') AS rows(id int, product_categ_code varchar, nyuka_ten_code varchar, nohn_bi date, qty float)) LOOP
            INSERT INTO inventory_fact (
                product_categ_code,
                store_code,
                date_key,
                total_qty
            ) VALUES (
                inventory_fact_source.product_categ_code,
                inventory_fact_source.nyuka_ten_code,
                TO_CHAR(inventory_fact_source.nohn_bi, 'yyyymmdd'),
                inventory_fact_source.qty
            );

            last_id = inventory_fact_source.id;
        END LOOP;

        -- update is_metabase_inventory_sync = true for receipt_oc
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE receipt_oc SET is_metabase_inventory_sync = true WHERE is_metabase_inventory_sync=false AND id <=' || last_id);
        

        -- select from disposal
        FOR inventory_fact_source IN (SELECT * FROM dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT d.id, mdp.product_categ, d.store_code, d.registration_date, d.waste_qty FROM disposal AS d INNER JOIN master_data_product AS mdp ON mdp.product_code=d.product_code WHERE d.is_metabase_inventory_sync=false ORDER BY d.id ASC') AS rows(id int, product_categ_code varchar, store_code varchar, registration_date date, waste_qty float)) LOOP
            INSERT INTO inventory_fact (
                product_categ_code,
                store_code,
                date_key,
                total_qty
            ) VALUES (
                inventory_fact_source.product_categ_code,
                inventory_fact_source.store_code,
                TO_CHAR(inventory_fact_source.registration_date, 'yyyymmdd'),
                (-1 * inventory_fact_source.waste_qty)
            );

            last_id = inventory_fact_source.id;
        END LOOP;

        -- update is_metabase_inventory_sync = true for disposal
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE disposal SET is_metabase_inventory_sync = true WHERE is_metabase_inventory_sync=false AND id <=' || last_id);
    END;
$function$

-- PROSES ETL
SELECT get_inventory_fact('middleware', '5432', '127.0.0.1', 'postgres', 'postgres');