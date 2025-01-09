import argparse
import psycopg2
import sys

# Database connection parameters
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": 5432
}

def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def run_apache_age_query(query):
    conn = connect_to_db()
    conn.autocommit = True
    cursor = conn.cursor()

    # Execute SQL commands
    cursor.execute("LOAD 'age';")
    cursor.execute("SET search_path TO ag_catalog;")
    cursor.execute(query);
    result = cursor.fetchall()
    conn.close()
    return result

def task_1(node_name):
    """1. znajduje wszystkie dzieci danego wezla"""
    query = f"""
        SELECT * FROM cypher('iw_graph', $$
            MATCH (n {{name: \'{node_name}\'}})-[e]->(child)
            RETURN child.name
        $$) AS result(n agtype);
    """
    print(query)
    print(run_apache_age_query(query))

def task_2(node_name):
    """2. zlicza wszystkie dzieci danego wezla"""
    query = f"""
        SELECT * FROM cypher('iw_graph', $$
            MATCH (n {{name: \'{node_name}\'}})-[e]->(child)
            RETURN COUNT(child) AS child_count
        $$) AS result(child_count agtype);
    """
    print(query)
    print(run_apache_age_query(query))

def task_3(node_name):
    """3. znajduje wszystkie wnuki danego wezla"""
    query = f"""
        SELECT * FROM cypher('iw_graph', $$
            MATCH (n {{name: \'{node_name}\'}})-[e]->(child)-[e2]->(grandchild)
            RETURN grandchild.name
        $$) AS result(name agtype);
    """
    print(query)
    print(run_apache_age_query(query))

def task_4(node_name):
    """4. znajduje wszystkich rodziców danego wezla"""
    query = f"""
        SELECT * FROM cypher('iw_graph', $$
            MATCH (parent)-[e]->(n {{name: \'{node_name}\'}})
            RETURN parent.name
        $$) AS result(name agtype);
    """
    print(query)
    print(run_apache_age_query(query))

def task_5(node_name):
    """5. zlicza wszystkich rodziców danego wezla"""
    query = f"""
        SELECT * FROM cypher('iw_graph', $$
            MATCH (parent)-[e]->(n {{name: \'{node_name}\'}})
            RETURN COUNT(parent) AS parent_count
        $$) AS result(parent_count int);
    """
    print(query)
    print(run_apache_age_query(query))

def task_6(node_name):
    """6. znajduje wszystkich dziadków danego wezla"""
    query = f"""
        SELECT * FROM cypher('iw_graph', $$
            MATCH (grandparent)-[e]->(parent)-[e2]->(n {{name: \'{node_name}\'}})
            RETURN grandparent.name
        $$) AS result(name agtype);
    """
    print(query)
    print(run_apache_age_query(query))

def task_7():
    """7. liczy, ile jest wezlów o unikatowych nazwach"""
    query = f"""
        SELECT COUNT(*) AS unique_node_count
        FROM cypher('iw_graph', $$
            MATCH (n)
            RETURN DISTINCT n.name
        $$) AS result(name agtype);
    """
    print(query)
    print(run_apache_age_query(query))


#### !!!! DZIALA TAK DLUGO ZE NWM CZY DZIALA !!!!
def task_8(): 
    """8. znajduje wezly, które nie sa podkategoria zadnego innego wezla"""
    query = f"""
        SELECT * FROM cypher('iw_graph', $$
            MATCH (n)
            WHERE NOT EXISTS (n)<-[]-()
            RETURN n.name
        $$) AS result(name agtype);
    """
    print(query)
    print(run_apache_age_query(query))

#### !!!! DZIALA TAK DLUGO ZE NWM CZY DZIALA (bo opiera sie na 8) !!!!
def task_9(): 
    """9. zlicza wezly z celu 8"""
    query = f"""
        SELECT COUNT(*) FROM cypher('iw_graph', $$
            MATCH (n)
            WHERE NOT EXISTS (n)<-[]-()
            RETURN n.name
        $$) AS result(name agtype);
    """
    print(query)
    print(run_apache_age_query(query))

def task_10():
    """10. znajduje wezly z największą liczbą dzieci, może być ich więcej"""
    query = f"""
        WITH aggregated_data AS ( 
            SELECT start_id, COUNT(end_id) AS num_childs 
            FROM iw_graph.has 
            GROUP BY start_id 
            ORDER BY num_childs DESC 
            LIMIT 1 
            )
        SELECT c.properties, a.num_childs 
        FROM aggregated_data a LEFT JOIN iw_graph."Category" c ON a.start_id = c.id;
    """
    print(query)
    print(run_apache_age_query(query))


## tutaj do poprawienia bo jest ich naprawdę dużo
def task_11():
    """11. znajduje węzły z najmniejszą liczbę dzieci (liczba dzieci jest większa od zera),"""
    query = f"""
        WITH aggregated_data AS ( 
            SELECT start_id, COUNT(end_id) AS num_childs 
            FROM iw_graph.has 
            GROUP BY start_id 
            ORDER BY num_childs 
            LIMIT 100 
            )
        SELECT c.properties, a.num_childs 
        FROM aggregated_data a LEFT JOIN iw_graph."Category" c ON a.start_id = c.id;
    """
    print(query)
    print(run_apache_age_query(query))
    pass

def task_12(old_name, new_name):
    # """12. Renames a given node"""
    query = f"""
        SELECT * FROM cypher('iw_graph', $$
            MATCH (n {{name: \'{old_name}\'}})
            SET n.name = \'{new_name}\'
            RETURN n
        $$) AS result(n agtype);
    """
    print(query)
    print(run_apache_age_query(query))

def task_13(node_name, new_popularity):
    # """13. Changes the popularity of a given node"""
    query = f"""
        SELECT * FROM cypher('iw_graph', $$
            MATCH (n {{name: \'{node_name}\'}})
            SET n.popularity = {new_popularity}
            RETURN n
        $$) AS result(n agtype);
    """
    print(query)
    print(run_apache_age_query(query))

def main(task_number, *args):
    if task_number == 1:
        task_1(args[0])
    elif task_number == 2:
        task_2(args[0])
    elif task_number == 3:
        task_3(args[0])
    elif task_number == 4:
        task_4(args[0])
    elif task_number == 5:
        task_5(args[0])
    elif task_number == 6:
        task_6(args[0])
    elif task_number == 7:
        task_7()
    elif task_number == 8:
        task_8()
    elif task_number == 9:
        task_9()
    elif task_number == 10:
        task_10()
    elif task_number == 11:
        task_11()
    elif task_number == 12:
        task_12(args[0], args[1])
    elif task_number == 13:
        task_13(args[0], args[1])
    else:
        print("Invalid goal number. Please provide a goal between 1 and 16")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python dbcli.py <integer> [additional arguments...]")
        sys.exit(1)

    try:
        task_number = int(sys.argv[1])
        args = sys.argv[2:]
        main(task_number, *args)
    except ValueError:
        print("Error: The first argument must be an integer.")