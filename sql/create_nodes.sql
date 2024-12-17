SELECT * FROM cypher('wiki_taxonomy_graph', $$
    UNWIND (SELECT DISTINCT category FROM taxonomy_temp) AS cat
    CREATE (:Category {name: cat.category})
$$);

SELECT * FROM cypher('wiki_taxonomy_graph', $$
    UNWIND (SELECT DISTINCT subcategory FROM taxonomy_temp) AS subcat
    CREATE (:Category {name: subcat.subcategory})
$$);
