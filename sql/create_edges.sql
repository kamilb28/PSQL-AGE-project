SELECT * FROM cypher('wiki_taxonomy_graph', $$
    MATCH (c:Category {name: t.category})
    MATCH (sc:Category {name: t.subcategory})
    CREATE (c)-[:HAS_SUBCATEGORY]->(sc)
$$, (SELECT category, subcategory FROM taxonomy_temp t));
