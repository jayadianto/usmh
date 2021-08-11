CREATE OR REPLACE FUNCTION public.generate_random_teraoka_sales_temp(total_row integer)
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        i INT;
		product_code_temp VARCHAR;
		store_code_temp VARCHAR;
    BEGIN
        FOR  i in 1..total_row LOOP
        	-- SELECT RANDOM product
        	SELECT product_code INTO product_code_temp FROM master_data_product TABLESAMPLE BERNOULLI(0.1) LIMIT 1;
			-- SELECT RANDOM STORE
			SELECT store_code INTO store_code_temp FROM odoo_server TABLESAMPLE BERNOULLI(50) LIMIT 1;
			
			-- INSERT TO teraoka_sales_temp random date +- 30 days, random qty 1-10
            INSERT INTO teraoka_sales_temp(
                product_code, 
                business_day_format, 
                clock, 
                qty, 
                store_code, 
                mark_as_done, 
                stage, 
                is_metabase_pos_sync, 
                is_metabase_sales_hour_sync, 
                is_metabase_inventory_sync,
                is_metabase_inventory_product_sync
            )
            VALUES(
                product_code_temp, 
                NOW() + (random() * (NOW()+'-30 days' - NOW())) + '-30 days', 
                NULL, 
                floor(random()* (10 - 1 + 1) + 1), 
                store_code_temp, 
                FALSE, 
                1, 
                FALSE, 
                FALSE, 
                FALSE,
                FALSE
            );
        END LOOP;
    END;
$function$

-- PROSES GENERATE, SAMPLE 1RB DATA
SELECT generate_random_teraoka_sales_temp(1000);