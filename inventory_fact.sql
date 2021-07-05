-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."teraoka_sales_temp" ADD COLUMN "is_metabase_inventory_sync" bool DEFAULT FALSE;
ALTER TABLE "public"."receipt_oc" ADD COLUMN "is_metabase_inventory_sync" bool DEFAULT FALSE;
ALTER TABLE "public"."disposal" ADD COLUMN "is_metabase_inventory_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_inventory_fact(db_name character varying, db_port character varying, db_host character varying, db_user character varying, db_password character varying)
RETURNS void
LANGUAGE plpgsql
AS $function$
    BEGIN
        -- select from teraoka_sales_temp
         INSERT INTO inventory_fact (
            product_categ_code,
            store_code,
            date_key,
            total_qty
        )
        SELECT
            product_categ_code,
            store_code,
            TO_CHAR(business_day_format, 'yyyymmdd'),
            (-1 * qty)
        FROM
            dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT mdp.product_categ, tst.store_code, date(tst.business_day_format) AS business_day_format, SUM(tst.qty) FROM teraoka_sales_temp AS tst INNER JOIN master_data_product AS mdp ON mdp.product_code=tst.product_code WHERE tst.is_metabase_inventory_sync=false GROUP BY mdp.product_categ, tst.store_code, business_day_format ORDER BY business_day_format ASC') AS rows(product_categ_code varchar, store_code varchar, business_day_format date, qty float);

        -- update is_metabase_inventory_sync = true for teraoka_sales_temp
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE teraoka_sales_temp SET is_metabase_inventory_sync = true WHERE is_metabase_inventory_sync=false');     
    

        -- select from receipt_oc
        INSERT INTO inventory_fact (
            product_categ_code,
            store_code,
            date_key,
            total_qty
        )
        SELECT
            product_categ_code,
            nyuka_ten_code,
            TO_CHAR(nohn_bi, 'yyyymmdd'),
            qty
        FROM
            dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT mdp.product_categ, ro.nyuka_ten_code, date(ro.nohn_bi) AS nohn_bi, SUM(ro.qty) FROM receipt_oc AS ro INNER JOIN master_data_product AS mdp ON mdp.product_code=ro.nyuka_sku_code WHERE ro.is_metabase_inventory_sync=false GROUP BY mdp.product_categ, ro.nyuka_ten_code, nohn_bi ORDER BY nohn_bi ASC') AS rows(product_categ_code varchar, nyuka_ten_code varchar, nohn_bi date, qty float);

        -- update is_metabase_inventory_sync = true for receipt_oc
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE receipt_oc SET is_metabase_inventory_sync = true WHERE is_metabase_inventory_sync=false');
        

        -- select from disposal
        INSERT INTO inventory_fact (
            product_categ_code,
            store_code,
            date_key,
            total_qty
        )
        SELECT
            product_categ_code,
            store_code,
            TO_CHAR(registration_date, 'yyyymmdd'),
            (-1 * waste_qty)
        FROM
            dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT mdp.product_categ, d.store_code, date(d.registration_date) AS registration_date, SUM(d.waste_qty) FROM disposal AS d INNER JOIN master_data_product AS mdp ON mdp.product_code=d.product_code WHERE d.is_metabase_inventory_sync=false GROUP BY mdp.product_categ, d.store_code, registration_date ORDER BY registration_date ASC') AS rows(product_categ_code varchar, store_code varchar, registration_date date, waste_qty float);

        -- update is_metabase_inventory_sync = true for disposal
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE disposal SET is_metabase_inventory_sync = true WHERE is_metabase_inventory_sync=false');
    END;
$function$

-- PROSES ETL
SELECT get_inventory_fact('middleware', '5432', '127.0.0.1', 'postgres', 'postgres');