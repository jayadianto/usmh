CREATE OR REPLACE FUNCTION public.generate_hour_series()
RETURNS void
LANGUAGE plpgsql
AS $function$
    BEGIN
        TRUNCATE TABLE hour_dim;

        INSERT INTO hour_dim
        SELECT 
            hour_series, 
            hour_series, 
            hour_series, 
            CASE WHEN hour_series > 12 THEN hour_series - 12 ELSE hour_series END AS hour_in_12, 
            CASE WHEN hour_series > 12 THEN 'pm' ELSE 'am' END AS am_pm
        FROM GENERATE_SERIES(1,24) AS hour_series;    
    END;
$function$

-- PROSES GENERATE DATA HOUR DIM
SELECT generate_hour_series();