DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM ag_catalog.ag_graph
        WHERE name = 'wiki_taxonomy_graph'
    ) THEN
        PERFORM create_graph('wiki_taxonomy_graph');
    END IF;
END $$;
