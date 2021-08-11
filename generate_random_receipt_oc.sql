CREATE OR REPLACE FUNCTION public.generate_random_receipt_oc(total_row integer)
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        i INT;
		product_temp RECORD;
		store_code_temp VARCHAR;
        purchase_number INT;
        receipt_date VARCHAR;
        purchase_qty INT;
    BEGIN
        FOR  i in 1..total_row LOOP
        	-- SELECT RANDOM product
        	SELECT product_code, department_categ, product_name_kanji, standard_selling_price, standard_cost FROM master_data_product INTO product_temp TABLESAMPLE BERNOULLI(0.1) LIMIT 1;
			
            -- SELECT RANDOM STORE
			SELECT store_code INTO store_code_temp FROM odoo_server TABLESAMPLE BERNOULLI(50) LIMIT 1;
			
            -- GENERATE RANDOM PURCHASE NUMBER
            purchase_number = floor((10 + 7*random())*(row_number() over()));

            -- GENERATE RANDOM QTY 1 - 10
            purchase_qty = floor(random()* (10 - 1 + 1) + 1);

            -- GENERATE RECEIPT DATE
            receipt_date = TO_CHAR(NOW() + (random() * (NOW()+'-30 days' - NOW())) + '-30 days', 'YYYY/MM/DD');
			
            -- INSERT TO receipt_oc
            INSERT INTO receipt_oc(
                syori_date, 
                den_betu_code, 
                den_no, 
                den_eda_no, 
                den_gyo_no, 
                shuka_ten_code, 
                nyuka_ten_code, 
                nohn_bi, 
                shuka_bunrui1_code,
                nyuka_bunrui1_code,
                shuka_sku_code,
                nyuka_sku_code,
                shuka_scan_code,
                nyuka_scan_code,
                item_rj,
                item_kbn,
                kazei_kbn,
                qty,
                sir_hon_gen_tnk,
                sir_gnk_kin,
                sir_hon_bai_tnk,
                sir_bika_kin,
                mark_as_done,
                stage,
                is_metabase_purchase_sync,
                is_metabase_inventory_sync,
                is_metabase_inventory_product_sync
            )
            VALUES(
                receipt_date,
                0, 
                purchase_number,
                0, 
                0, 
                0000, 
                store_code_temp,
                receipt_date,
                product_temp.department_categ,
                product_temp.department_categ,
                product_temp.product_code,
                product_temp.product_code,
                NULL,
                NULL, 
                product_temp.product_name_kanji,
                1,
                0,
                purchase_qty, 
                product_temp.standard_cost,
                (purchase_qty * CAST(product_temp.standard_cost AS FLOAT)),
                product_temp.standard_selling_price,
                (purchase_qty * CAST(product_temp.standard_selling_price AS FLOAT)),
                FALSE,
                1,
                FALSE,
                FALSE,
                FALSE
            );
        END LOOP;
    END;
$function$

-- PROSES GENERATE, SAMPLE 1RB DATA
SELECT generate_random_receipt_oc(1000);