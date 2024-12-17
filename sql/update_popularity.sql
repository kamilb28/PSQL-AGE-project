SELECT * FROM cypher('wiki_taxonomy_graph', $$
    MATCH (n:Category {name: t.node_name})
    SET n.popularity = t.popularity
$$, (SELECT node_name, popularity FROM popularity_temp t));
