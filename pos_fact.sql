-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."teraoka_sales_temp" ADD COLUMN "is_metabase_pos_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_pos_fact(db_name character varying, db_port character varying, db_host character varying, db_user character varying, db_password character varying)
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        pos_fact_source RECORD;
        last_id INT;
    BEGIN
        FOR pos_fact_source IN (SELECT * FROM dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT tst.id, mdp.product_categ, tst.store_code, tst.business_day_format, tst.qty FROM teraoka_sales_temp AS tst INNER JOIN master_data_product AS mdp ON mdp.product_code=tst.product_code WHERE tst.is_metabase_pos_sync=false ORDER BY tst.id ASC') AS rows(id int, product_categ_code varchar, store_code varchar, business_day_format date, qty float)) LOOP
            INSERT INTO pos_fact (
                product_categ_code,
                store_code,
                date_key,
                total_qty,
                sales_amount
            ) VALUES (
                pos_fact_source.product_categ_code,
                pos_fact_source.store_code,
                TO_CHAR(pos_fact_source.business_day_format, 'yyyymmdd'),
                pos_fact_source.qty,
                1000 -- dummy field sales_amount not found in table teraoka_sales_temp
            );

            last_id = pos_fact_source.id;
        END LOOP;

        -- update is_metabase_pos_sync = true
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE teraoka_sales_temp SET is_metabase_pos_sync = true WHERE is_metabase_pos_sync=false AND id <=' || last_id);     
    END;
$function$

-- PROSES ETL
SELECT get_pos_fact('middleware', '5432', '127.0.0.1', 'postgres', 'postgres');