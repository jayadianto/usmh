-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."product_category_master" ADD COLUMN "is_metabase_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_product_categ_dim(db_name character varying, db_port character varying, db_host character varying, db_user character varying, db_password character varying)
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        product_categ_dim_source RECORD;
        last_id INT;
        rows_found INT;
    BEGIN
        FOR product_categ_dim_source IN (SELECT * FROM dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'SELECT pcm.id, pcm.unique_key, pcm.department, pcm.classification_kana, pcm.classification_kanji, pcm.variety_kana, pcm.variety_kanji, pcm.product_group_kana, pcm.product_group_kanji, pm.unit_unit, pm.storage_life, mdp.standard_cost FROM product_category_master AS pcm INNER JOIN master_data_product AS mdp ON mdp.product_categ = pcm.unique_key INNER JOIN product_master AS pm ON pm.id = mdp.product_middleware_id WHERE pcm.is_metabase_sync = false ORDER BY pcm.id ASC ') AS rows(product_categ_id int, product_categ_code varchar, product_categ_dept_kanji varchar, product_categ_class_kana varchar, product_categ_class_kanji varchar, product_categ_variety_kana varchar, product_categ_variety_kanji varchar, product_categ_product_group_kana varchar, product_categ_product_group_kanji varchar, product_uom_name varchar, standard_cost varchar, storage_life varchar)) LOOP
            -- check if unique key has created
            -- SELECT COUNT(*) INTO rows_found FROM product_categ_dim WHERE product_categ_code = product_categ_dim_source.product_categ_code;
            
            -- if not created , then create new
            -- IF rows_found = 0 THEN
                INSERT INTO product_categ_dim (
                    product_categ_code,
                    product_categ_dept_kanji,
                    product_categ_class_kana,
                    product_categ_class_kanji,
                    product_categ_variety_kana,
                    product_categ_variety_kanji,
                    product_categ_product_group_kana,
                    product_categ_product_group_kanji,
                    product_uom_name,
                    standard_cost,
                    storage_life
                ) VALUES (
                    product_categ_dim_source.product_categ_code,
                    product_categ_dim_source.product_categ_dept_kanji,
                    product_categ_dim_source.product_categ_class_kana,
                    product_categ_dim_source.product_categ_class_kanji,
                    product_categ_dim_source.product_categ_variety_kana,
                    product_categ_dim_source.product_categ_variety_kanji,
                    product_categ_dim_source.product_categ_product_group_kana,
                    product_categ_dim_source.product_categ_product_group_kanji,
                    product_categ_dim_source.product_uom_name,
                    product_categ_dim_source.standard_cost,
                    product_categ_dim_source.storage_life
                );
            -- END IF;

            last_id = product_categ_dim_source.product_categ_id;
        END LOOP;

        -- update is_metabase_sync = true
        PERFORM  dblink('dbname='||db_name||' port='||db_port||' host='||db_host||' user='||db_user||' password='||db_password, 'UPDATE product_category_master SET is_metabase_sync = true WHERE is_metabase_sync=false AND id <=' || last_id);     
    END;
$function$

-- PROSES ETL
SELECT get_product_categ_dim('middleware', '5432', '127.0.0.1', 'postgres', 'postgres');