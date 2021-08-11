-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."teraoka_sales_temp" ADD COLUMN "is_metabase_sales_hour_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_sales_hour_fact(db_name character varying, db_port character varying, db_host character varying, db_user character varying, db_password character varying)
RETURNS void
LANGUAGE plpgsql
AS $function$
    BEGIN
         INSERT INTO sales_hour_fact (
            product_code,
            store_code,
            date_key,
            total_qty,
            sales_amount,
            number_of_customer,
            hour_key
        )
        SELECT
            product_code,
            store_code,
            TO_CHAR(date_pos, 'yyyymmdd'),
            qty,
            1000, -- dummy field sales_amount not found in table teraoka_sales_temp,
            1, -- dummy field number_of_customer not found in table teraoka_sales_temp,
            hour_key
        FROM
            dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT product_code, store_code, date(business_day_format) AS date_pos, SUM(qty), CASE WHEN (CAST(TO_CHAR(business_day_format, ''MI'') AS INTEGER) > 30) THEN (CAST(TO_CHAR(business_day_format, ''HH'') AS INTEGER) + 1) ELSE CAST(TO_CHAR(business_day_format, ''HH'') AS INTEGER) END AS hour_key FROM teraoka_sales_temp WHERE is_metabase_sales_hour_sync=false GROUP BY product_code, store_code, date_pos, hour_key ORDER BY date_pos ASC;
') AS rows(product_code varchar, store_code varchar, date_pos date, qty float, hour_key integer);

        -- update is_metabase_sales_hour_sync = true
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE teraoka_sales_temp SET is_metabase_sales_hour_sync = true WHERE is_metabase_sales_hour_sync=false');     
    END;
$function$

-- PROSES ETL
SELECT get_sales_hour_fact('middleware', '5432', '127.0.0.1', 'postgres', 'postgres');