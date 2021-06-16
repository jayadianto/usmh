CREATE OR REPLACE FUNCTION public.dblink(text, text)
 RETURNS SETOF record
 LANGUAGE c
 PARALLEL RESTRICTED STRICT
AS '$libdir/dblink', $function$dblink_record$function$
