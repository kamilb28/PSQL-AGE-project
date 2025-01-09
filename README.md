## Obraz dokera

`docker pull apache/age`

`docker run --name age-container -e POSTGRES_PASSWORD=$haslo -p 5432:5432 -d apache/age`

## Skrypt

import_directly dla importu pojedynczo po kazdym wierszu csv(dziala mega dlugo)

import_taxonomy dla stowrzenia tabel tymczasowych psql i z nich grafu w age (nie dziala)

## Komendy

`docker exec -it age psql -U postgres`

`\c wikipedia_taxonomy`

`LOAD 'age';`

`SET search_path = ag_catalog, "$user", public;`

`SELECT * FROM cypher('wiki_taxonomy_graph', $$ MATCH (n:Category) RETURN n.name $$) AS (name agtype);`

`SELECT * FROM cypher('wiki_taxonomy_graph', $$
    MATCH (c:Category)-[rel:SUBCATEGORY]->(sc:Category)
    RETURN c.name, sc.name
$$) AS (c agtype, sc agtype);`

`SELECT * FROM drop_graph('wiki_taxonomy_graph', true);`



### add more resorces 
`docker update --memory=2g --memory-swap=3g age-container`