-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."teraoka_sales_temp" ADD COLUMN "is_metabase_pos_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_pos_fact(db_name character varying, db_port character varying, db_host character varying, db_user character varying, db_password character varying)
RETURNS void
LANGUAGE plpgsql
AS $function$
    BEGIN
         INSERT INTO pos_fact (
            product_categ_code,
            store_code,
            date_key,
            total_qty,
            sales_amount
        )
        SELECT
            product_categ_code,
            store_code,
            TO_CHAR(date_pos, 'yyyymmdd'),
            qty,
            1000 -- dummy field sales_amount not found in table teraoka_sales_temp
        FROM
            dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT mdp.product_categ, tst.store_code, date(tst.business_day_format) AS date_pos, SUM(tst.qty) FROM teraoka_sales_temp AS tst INNER JOIN master_data_product AS mdp ON mdp.product_code=tst.product_code WHERE tst.is_metabase_pos_sync=false GROUP BY mdp.product_categ, tst.store_code, date_pos ORDER BY date_pos ASC;
') AS rows(product_categ_code varchar, store_code varchar, date_pos date, qty float);

        -- update is_metabase_pos_sync = true
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE teraoka_sales_temp SET is_metabase_pos_sync = true WHERE is_metabase_pos_sync=false');     
    END;
$function$

-- PROSES ETL
SELECT get_pos_fact('middleware', '5432', '127.0.0.1', 'postgres', 'postgres');