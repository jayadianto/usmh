-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."receipt_oc" ADD COLUMN "is_metabase_purchase_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_purchase_fact(db_name character varying, db_port character varying, db_host character varying, db_user character varying, db_password character varying)
RETURNS void
LANGUAGE plpgsql
AS $function$
    BEGIN
        INSERT INTO purchase_fact (
            product_categ_code,
            store_code,
            date_key,
            purchase_number,
            total_qty
        )
        SELECT
            product_categ_code,
            nyuka_ten_code,
            TO_CHAR(nohn_bi, 'yyyymmdd'),
            den_no,
            qty
        FROM
            dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT mdp.product_categ, ro.nyuka_ten_code, DATE(ro.nohn_bi) AS nohn_bi, ro.den_no, SUM(ro.qty) FROM receipt_oc AS ro INNER JOIN master_data_product AS mdp ON mdp.product_code=ro.nyuka_sku_code WHERE ro.is_metabase_purchase_sync=false GROUP BY mdp.product_categ, ro.nyuka_ten_code, nohn_bi, ro.den_no ORDER BY nohn_bi ASC') AS rows(product_categ_code varchar, nyuka_ten_code varchar, nohn_bi date, den_no varchar, qty float);

        -- update is_metabase_purchase_sync = true
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE receipt_oc SET is_metabase_purchase_sync = true WHERE is_metabase_purchase_sync=false');     
    END;
$function$

-- PROSES ETL
SELECT get_purchase_fact('middleware', '5432', '127.0.0.1', 'postgres', 'postgres');