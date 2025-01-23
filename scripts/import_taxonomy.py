import gzip
import psycopg2
import csv
import os

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


def create_database_if_not_exists():
    """Creates the database if it does not exist."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_CONFIG['dbname']}'")
            exists = cur.fetchone()
            if not exists:
                cur.execute(f"CREATE DATABASE {DB_CONFIG['dbname']}")
                print(f"Database '{DB_CONFIG['dbname']}' created successfully.")
            else:
                print(f"Database '{DB_CONFIG['dbname']}' already exists.")

    except Exception as e:
        print(f"Error while creating database: {e}")

    finally:
        if conn:
            conn.close()


def execute_sql_file(conn, filepath):
    with open(filepath, 'r') as file:
        sql = file.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"Wykonano plik SQL: {filepath}")


def import_gz_to_table(conn, gz_file, table_name):
    csv_file = gz_file.replace('.gz', '')
    
    if not os.path.exists(csv_file):
        with gzip.open(gz_file, 'rt', encoding='utf-8') as f_in, open(csv_file, 'w', encoding='utf-8') as f_out:
            f_out.writelines(f_in)
        print(f"Plik {gz_file} został rozpakowany.")

    with conn.cursor() as cur, open(csv_file, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f, quotechar='"', delimiter=',', escapechar='\\')
        print("INSERTING...")
        for row in reader:
            try: 
                cur.execute(f"INSERT INTO {table_name} VALUES (%s, %s)", row)
            except Exception as e:
                print(f"Error inserting: {row}, error: {e}")
                continue
    conn.commit()
    print(f"Dane z {csv_file} zostały zaimportowane do tabeli {table_name}.")


def main():
    create_database_if_not_exists()
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Połączono z bazą danych.")

        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_extension.sql'))
        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_tables.sql'))
        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_graph.sql'))

        import_gz_to_table(conn, TAXONOMY_FILE, 'taxonomy_temp')
        import_gz_to_table(conn, POPULARITY_FILE, 'popularity_temp')

        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_nodes.sql'))
        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_edges.sql'))

        execute_sql_file(conn, os.path.join(SQL_DIR, 'update_popularity.sql'))

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()
            print("DB connection closed")


if __name__ == "__main__":
    main()
