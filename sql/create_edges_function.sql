CREATE OR REPLACE FUNCTION public.create_nodes(name text)
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $BODY$
BEGIN
    load 'age';
    SET search_path TO ag_catalog;
    EXECUTE format('SELECT * FROM cypher(''wiki_taxonomy_graph'', $$CREATE (:Category {name: %s})$$) AS (a agtype);', quote_ident(name));
END
$BODY$;