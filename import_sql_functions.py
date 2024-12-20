import gzip
import psycopg2
import csv
import os

from import_taxonomy import create_database_if_not_exists, execute_sql_file, DB_CONFIG, SQL_DIR, TAXONOMY_FILE, POPULARITY_FILE
from clean_csv import clean_csv

def unpack_and_clean(gz_file):
    csv_file = gz_file.replace('.gz', '')
    csv_clean = csv_file.replace('_iw', '_iw_clean')

    if not os.path.exists(csv_file):
        with gzip.open(gz_file, 'rt', encoding='utf-8') as f_in, open(csv_file, 'w', encoding='utf-8') as f_out:
            f_out.writelines(f_in)
        print(f"Plik {gz_file} został rozpakowany.")

    if not os.path.exists(csv_clean):
        csv_file(csv_file, csv_clean)
        print(f"Plik {gz_file} został wyczyszczony.")

    return csv_clean

def import_csv_to_table(conn, file, table_name):

    with conn.cursor() as cur, open(file, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f, quotechar='"', delimiter=',', escapechar='\\')
        print("INSERTING...")
        for row in reader:
            try: 
                cur.execute(f"INSERT INTO {table_name} VALUES (%s, %s)", row)
            except Exception as e:
                print(f"Error inserting: {row}, error: {e}")
                continue
    conn.commit()
    print(f"Dane z {file} zostały zaimportowane do tabeli {table_name}.")

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

        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_nodes_function.sql'))
        #execute_sql_file(conn, os.path.join(SQL_DIR, 'execute_create_nodes_function.sql'))

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()
            print("DB connection closed")


if __name__ == "__main__":
    main()