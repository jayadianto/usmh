CREATE OR REPLACE FUNCTION public.generate_random_disposal(total_row integer)
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        i INT;
		product_temp RECORD;
        waste_qty INT;
        scan_code VARCHAR;
        registration_date VARCHAR;
        registration_time VARCHAR;
        store_code_temp VARCHAR;
    BEGIN
        FOR  i in 1..total_row LOOP
        	-- SELECT RANDOM product
        	SELECT product_code, standard_selling_price FROM master_data_product INTO product_temp TABLESAMPLE BERNOULLI(0.1) LIMIT 1;
			
            -- SELECT RANDOM STORE
			SELECT store_code INTO store_code_temp FROM odoo_server TABLESAMPLE BERNOULLI(50) LIMIT 1;
			
            -- GENERATE RANDOM SCAN CODE
            scan_code = floor(random()* (9000000000000 - 4000000000000 + 4000000000000) + 4000000000000);

            -- GENERATE RANDOM QTY 1 - 10
            waste_qty = floor(random()* (10 - 1 + 1) + 1);

            -- GENERATE REGISTRATION DATE
            registration_date = TO_CHAR(NOW() + (random() * (NOW()+'-30 days' - NOW())) + '-30 days', 'YYYYMMDD');

            -- GENERATE REGISTRATION TIME
            registration_time = TO_CHAR(NOW() + (random() * (NOW()+'-12 hours' - NOW())) + '-12 hours', 'HHMI');
			
            -- INSERT TO disposal
            INSERT INTO disposal(
                store_code, 
                registration_date, 
                registration_time, 
                product_code, 
                scan_code, 
                waste_qty, 
                disposal_unit_price, 
                total_amount, 
                mark_as_done,
                stage,
                is_metabase_inventory_sync,
                is_metabase_inventory_product_sync
            )
            VALUES(
                store_code_temp, 
                registration_date, 
                registration_time, 
                product_temp.product_code, 
                scan_code, 
                waste_qty, 
                CAST(product_temp.standard_selling_price AS FLOAT), 
                (waste_qty * CAST(product_temp.standard_selling_price AS FLOAT)), 
                FALSE,
                1,
                FALSE,
                FALSE
            );
        END LOOP;
    END;
$function$

-- PROSES GENERATE, SAMPLE 1RB DATA
SELECT generate_random_disposal(1000);