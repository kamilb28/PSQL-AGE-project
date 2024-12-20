import gzip
import psycopg2
import csv
import os
from tqdm import tqdm
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


def sanitize_csv(gz_file):
    """Sanitize the CSV file by replacing escape characters with empty strings."""
    sanitized_file = gz_file.replace('.gz', '_sanitized.csv')
    original_file = gz_file.replace('.gz', '')

    if not os.path.exists(original_file):
        with gzip.open(gz_file, 'rt', encoding='utf-8') as f_in, open(original_file, 'w', encoding='utf-8') as f_out:
            f_out.writelines(f_in)
        print(f"Decompressed {gz_file} to {original_file}")

    with open(original_file, 'r', encoding='utf-8') as infile, open(sanitized_file, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile, quotechar='"', delimiter=',', escapechar='\\')
        writer = csv.writer(outfile, quotechar='"', delimiter=',', escapechar='\\')
        for row in tqdm(reader, desc="Sanitizing CSV"):
            sanitized_row = [col.replace("\\", "").strip() for col in row]  # Replace escape characters
            writer.writerow(sanitized_row)
    print(f"Sanitized file created: {sanitized_file}")
    return sanitized_file


def import_nodes_and_edges(conn, gz_taxonomy_file):
    """Import nodes and edges directly into the graph."""
    csv_file = sanitize_csv(gz_taxonomy_file)  # Sanitize the file before processing

    with conn.cursor() as cur, open(csv_file, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f, quotechar='"', delimiter=',', escapechar='\\')
        total_rows = sum(1 for _ in open(csv_file, 'r', encoding='utf-8'))  # Count total rows for tqdm
        f.seek(0)  # Reset file pointer
        print("CREATE NODES...")

        for row in tqdm(reader, desc="Processing rows", total=total_rows):
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

    print(f"Data from {csv_file} has been imported into the graph.")


def set_popularity(conn, gz_popularity_file):
    """Set popularity property for nodes in the graph."""
    csv_file = sanitize_csv(gz_popularity_file)  # Sanitize the file before processing

    with conn.cursor() as cur, open(csv_file, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f, quotechar='"', delimiter=',', escapechar='\\')
        total_rows = sum(1 for _ in open(csv_file, 'r', encoding='utf-8'))  # Count total rows for tqdm
        f.seek(0)  # Reset file pointer
        print("SET POPULARITY...")

        for row in tqdm(reader, desc="Processing rows", total=total_rows):
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

    print(f"Data from {csv_file} has been imported into the graph.")


def main():
    create_database_if_not_exists()
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connected to the database.")

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

