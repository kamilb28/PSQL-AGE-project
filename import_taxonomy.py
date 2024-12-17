import gzip
import psycopg2
import csv
import os

# Parametry połączenia z bazą danych PostgreSQL
DB_CONFIG = {
    'dbname': 'wikipedia_taxonomy',  # Nazwa bazy danych
    'user': 'postgres',              # Użytkownik PostgreSQL
    'password': 'your_password',     # Hasło użytkownika
    'host': 'localhost',             # Host (np. localhost lub IP kontenera)
    'port': 5432                     # Port PostgreSQL (domyślnie 5432)
}

# Ścieżki do plików
TAXONOMY_FILE = 'taxonomy_iw.csv.gz'
POPULARITY_FILE = 'popularity_iw.csv.gz'
SQL_DIR = 'sql_queries'


def execute_sql_file(conn, filepath):
    """Funkcja wykonująca zapytania SQL z pliku."""
    with open(filepath, 'r') as file:
        sql = file.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"Wykonano plik SQL: {filepath}")


def import_gz_to_table(conn, gz_file, table_name):
    """Importuje dane z pliku .gz do tabeli PostgreSQL."""
    csv_file = gz_file.replace('.gz', '')
    
    # Rozpakuj plik gz
    with gzip.open(gz_file, 'rt', encoding='utf-8') as f_in, open(csv_file, 'w', encoding='utf-8') as f_out:
        f_out.writelines(f_in)
    print(f"Plik {gz_file} został rozpakowany.")

    # Importuj dane do tabeli
    with conn.cursor() as cur, open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Pominięcie nagłówka
        for row in reader:
            cur.execute(f"INSERT INTO {table_name} VALUES (%s, %s)", row)
    conn.commit()
    print(f"Dane z {gz_file} zostały zaimportowane do tabeli {table_name}.")

    # Usuń rozpakowany plik
    os.remove(csv_file)


def main():
    try:
        # Połączenie z bazą danych
        conn = psycopg2.connect(**DB_CONFIG)
        print("Połączono z bazą danych.")

        # Tworzenie rozszerzenia AGE i konfiguracja grafu
        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_extension.sql'))
        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_tables.sql'))
        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_graph.sql'))

        # Importowanie danych z plików
        import_gz_to_table(conn, TAXONOMY_FILE, 'taxonomy_temp')
        import_gz_to_table(conn, POPULARITY_FILE, 'popularity_temp')

        # Tworzenie węzłów i krawędzi w grafie
        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_nodes.sql'))
        execute_sql_file(conn, os.path.join(SQL_DIR, 'create_edges.sql'))

        # Aktualizacja popularności w węzłach grafu
        execute_sql_file(conn, os.path.join(SQL_DIR, 'update_popularity.sql'))

    except Exception as e:
        print(f"Wystąpił błąd: {e}")

    finally:
        if conn:
            conn.close()
            print("Połączenie z bazą danych zostało zamknięte.")


if __name__ == "__main__":
    main()
