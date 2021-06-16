-- EXEC DI DATABASE MIDDLEWARE
ALTER TABLE "public"."master_data_product" ADD COLUMN "is_metabase_sync" bool DEFAULT FALSE;

-- EXEC DI DATABASE METABASE
CREATE OR REPLACE FUNCTION public.get_product_dim()
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        product_dim_source RECORD;
        last_id INT;
    BEGIN
        FOR product_dim_source IN (SELECT * FROM dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'SELECT mdp.product_id, mdp.product_name_kanji, mdp.product_code, pm.unit_unit FROM master_data_product AS mdp INNER JOIN product_master AS pm ON pm.id=mdp.product_middleware_id WHERE is_metabase_sync=false ORDER BY product_id ASC') AS rows(product_id int, product_name_kanji varchar, product_code varchar, unit_unit varchar)) LOOP
            INSERT INTO product_dim (
                product_code,
                product_name,
                product_categ_dept,
                product_categ_class,
                product_categ_variety,
                product_categ_product_group,
                product_uom_name
            ) VALUES (
                product_dim_source.product_code,
                product_dim_source.product_name_kanji,
                'categ dept',
                'categ class',
                'categ variety',
                'categ product group',
                product_dim_source.unit_unit
            );

            last_id = product_dim_source.product_id;
        END LOOP;

        -- update is_metabase_sync = true
        PERFORM  dblink('dbname=middleware port=5432 host=127.0.0.1 user=postgres password=postgres', 'UPDATE master_data_product SET is_metabase_sync = true WHERE is_metabase_sync=false AND product_id <=' || last_id);     
    END;
$function$