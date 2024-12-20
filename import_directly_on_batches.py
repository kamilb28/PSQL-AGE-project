import gzip
import psycopg2
import csv
import os
import json
import age
import re

from import_taxonomy import create_database_if_not_exists, execute_sql_file

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

BATCH_SIZE = 5

def import_nodes_and_edges(conn, gz_taxonomy_file):

    csv_file = gz_taxonomy_file.replace('.gz', '')
    
    if not os.path.exists(csv_file):
        with gzip.open(gz_taxonomy_file, 'rt', encoding='utf-8') as f_in, open(csv_file, 'w', encoding='utf-8') as f_out:
            f_out.writelines(f_in)
        print(f"Plik {gz_taxonomy_file} został rozpakowany.")

    with conn.cursor() as cur, open(csv_file, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f, quotechar='"', delimiter=',', escapechar='\\')
        print("CREATE NODES IN BATCHES...")
        batch = []
        batch_data = ""

        def insert_batch(batch_data):
            if not batch_data:
                return
            #https://github.com/apache/age/issues/918
            query = f"""
                    SELECT * FROM cypher('wiki_taxonomy_graph', $$

                    WITH [{batch_data}] AS batch_data

                    UNWIND batch_data AS row
                    MERGE (c:Category {{name: row.a}})
                    MERGE (sc:Category {{name: row.b}})
                    MERGE (c)-[rel:SUBCATEGORY]->(sc)
                    RETURN rel
                    $$) AS (a agtype);
                """
            
            print(query)

            cur.execute(query)

        for row in reader:
            category, subcategory = row
            batch.append({"a": category, "b": subcategory})
            batch_data += f"{{a: \"{category}\", b: \"{subcategory}\"}},"
            
            if len(batch) >= BATCH_SIZE:
                insert_batch(batch_data[:-1])
                batch.clear()
                batch_data = ""
                conn.commit()
                return

        # remaining rows in the last partial batch
        insert_batch(batch_data[:-1])
    
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
        print("SET POPULARITY IN BATCHES...")

        batch = []
        batch_data = ""

        def update_popularity_batch(batch_data):
            if not batch_data:
                return
            
            query = f"""
                    SELECT * FROM cypher('wiki_taxonomy_graph', $$

                    WITH [{batch_data}] AS batch_data

                    UNWIND batch_data AS row

                    MERGE (c:Category {{name: row.name}})
                    SET c.popularity = row.popularity
                    RETURN count(*) AS a
                    $$) AS (a agtype);
                """
            #print(query)
            cur.execute(query)

        for row in reader:
            category_name, popularity = row
            category_name = re.sub(r'["\'$]', '', category_name)

            batch.append({"name": category_name, "popularity": popularity})
            batch_data += f"{{name: \"{category_name}\", popularity: {popularity}}},"
            
            if len(batch) >= 50000:
                update_popularity_batch(batch_data[:-1])
                batch.clear()
                batch_data = ""
                #conn.commit()
                #return

        # remaining rows in the last partial batch
        update_popularity_batch(batch_data[:-1])
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