import gzip
import psycopg2
import csv
import os
import json

from import_taxonomy import create_database_if_not_exists, execute_sql_file


"""
Import data directly into the graph from a gzipped CSV.

For taxonomy:
Each row: category, subcategory
We MERGE two nodes and a relationship.

For popularity:
Each row: node_name, popularity
We MERGE node and set a popularity property.
"""

DB_CONFIG = {
    'dbname': 'wikipedia_taxonomy',
    'user': 'postgres',
    'password': 'root',  # edit
    'host': 'localhost',
    'port': 5432
}

TAXONOMY_FILE = 'taxonomy_iw.csv.gz'
POPULARITY_FILE = 'popularity_iw.csv.gz'
SQL_DIR = 'sql'


def import_nodes_and_edges(conn, gz_taxonomy_file):

    csv_file = gz_taxonomy_file.replace('.gz', '')
    
    if not os.path.exists(csv_file):
        with gzip.open(gz_taxonomy_file, 'rt', encoding='utf-8') as f_in, open(csv_file, 'w', encoding='utf-8') as f_out:
            f_out.writelines(f_in)
        print(f"Plik {gz_taxonomy_file} został rozpakowany.")

    with conn.cursor() as cur, open(csv_file, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f, quotechar='"', delimiter=',', escapechar='\\')
        print("CREATE NODES...")

        for row in reader:
            category, subcategory = row
            cypher_query = f"""
                SELECT * FROM cypher('wiki_taxonomy_graph', 
                $$
                MERGE (c:Category {{name: "{category}"}})
                MERGE (sc:Category {{name: "{subcategory}"}})
                MERGE (c)-[:SUBCATEGORY]->(sc)
                RETURN c, sc
                $$) AS (c agtype, sc agtype);
            """
            cur.execute(cypher_query)

        conn.commit()

    print(f"Dane z {csv_file} zostały zaimportowane do grafu.")


def set_popularity(conn, gz_popularity_file):

    csv_file = gz_popularity_file.replace('.gz', '')
    
    if not os.path.exists(csv_file):
        with gzip.open(gz_popularity_file, 'rt', encoding='utf-8') as f_in, open(csv_file, 'w', encoding='utf-8') as f_out:
            f_out.writelines(f_in)
        print(f"Plik {gz_popularity_file} został rozpakowany.")

    with conn.cursor() as cur, open(csv_file, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f, quotechar='"', delimiter=',', escapechar='\\')
        print("SET POPULARITY...")

        for row in reader:
            category, popularity = row

            popularity = int(popularity) if popularity.isdigit() else 0
            cypher_query = f"""
                SELECT * FROM cypher('wiki_taxonomy_graph',
                $$
                MATCH (n:Category {{name: "{category}"}})
                SET n.popularity = {popularity}
                RETURN n
                $$) AS (n agtype);
            """

            cur.execute(cypher_query)
        conn.commit()

    print(f"Dane z {csv_file} zostały zaimportowane do grafu.")

def main():
    create_database_if_not_exists()
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Połączono z bazą danych.")

        # Create extension and graph
        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_extension.sql'))
        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_graph.sql'))

        # Direct import to graph
        import_nodes_and_edges(conn, TAXONOMY_FILE)
        set_popularity(conn, POPULARITY_FILE)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()
            print("DB connection closed")


if __name__ == "__main__":
    main()
