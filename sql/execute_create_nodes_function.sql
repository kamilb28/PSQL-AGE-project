SELECT public.create_nodes(taxonomy.category) 
FROM ag_catalog.taxonomy_temp AS taxonomy;

SELECT public.create_nodes(taxonomy.subcategory) 
FROM ag_catalog.taxonomy_temp AS taxonomy;