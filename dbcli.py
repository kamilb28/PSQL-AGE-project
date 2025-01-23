import argparse
import psycopg2
import sys
import json
from math import ceil
from tqdm import tqdm

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
    try:
        cursor.execute("LOAD 'age';")
        cursor.execute("SET search_path TO ag_catalog;")
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        conn.close()

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

def task_8():
    query_total = """
        SELECT total_n
        FROM cypher('iw_graph', $$
            MATCH (n)
            RETURN COUNT(n) AS total_n
        $$) AS (total_n agtype);
    """
    result_total = run_apache_age_query(query_total)
    total_nodes = int(str(result_total[0][0]).strip('"'))
    
    PAGE_SIZE = 500000  # best by tests 
    num_pages = ceil(total_nodes / PAGE_SIZE)

    no_inbound_ids = []

    current_offset = 0

    for _ in tqdm(range(num_pages), desc="Retrieving & checking node pages"):
        query_page = f"""
            SELECT no_inbound_id
            FROM cypher('iw_graph', $$
                /* Step 1: Match all nodes */
                MATCH (n)

                /* Step 2: Transition to WITH so we can apply ORDER, SKIP, LIMIT */
                WITH n
                ORDER BY id(n)
                SKIP {current_offset}
                LIMIT {PAGE_SIZE}

                /* Step 3: Collect them into a list for optional match */
                WITH collect(n) AS page_nodes

                /* Step 4: UNWIND and do OPTIONAL MATCH to check inbound edges */
                UNWIND page_nodes AS candidate
                OPTIONAL MATCH (m)-[r]->(candidate)
                WITH candidate, COUNT(m) AS inbound_count

                /* Step 5: Keep only those with zero inbound edges */
                WHERE inbound_count = 0
                RETURN id(candidate) AS no_inbound_id
            $$) AS (no_inbound_id agtype);
        """

        page_result = run_apache_age_query(query_page)

        for row in page_result:
            raw_str = str(row[0]).strip('"')
            no_inbound_ids.append(int(raw_str))

        current_offset += PAGE_SIZE

    name_chunk_size = 500000
    final_named = []

    def chunker(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i:i+size]

    for sub_ids in tqdm(list(chunker(no_inbound_ids, name_chunk_size)), desc="Retrieving names"):
        id_list_str = ", ".join(str(x) for x in sub_ids)
        query_names = f"""
            SELECT node_id, node_name
            FROM cypher('iw_graph', $$
                MATCH (n)
                WHERE id(n) IN [{id_list_str}]
                RETURN id(n) AS node_id, n.name AS node_name
            $$) AS (node_id agtype, node_name agtype);
        """
        sub_result = run_apache_age_query(query_names)

        for row in sub_result:
            raw_id = str(row[0]).strip('"')
            raw_name = str(row[1]).strip('"')
            final_named.append((int(raw_id), raw_name))

    for nid, nm in final_named:
        print(f" - Node ID={nid}, name={nm}")

def task_9():
    query_total = """
        SELECT total_n
        FROM cypher('iw_graph', $$
            MATCH (n)
            RETURN COUNT(n) AS total_n
        $$) AS (total_n agtype);
    """
    result_total = run_apache_age_query(query_total)
    total_nodes = int(str(result_total[0][0]).strip('"'))
    
    PAGE_SIZE = 500000  # bests by tests 
    num_pages = ceil(total_nodes / PAGE_SIZE)

    no_inbound_ids = []

    current_offset = 0

    for _ in tqdm(range(num_pages), desc="Retrieving & checking node pages"):
        query_page = f"""
            SELECT no_inbound_id
            FROM cypher('iw_graph', $$
                /* Step 1: Match all nodes */
                MATCH (n)

                /* Step 2: Transition to WITH so we can apply ORDER, SKIP, LIMIT */
                WITH n
                ORDER BY id(n)
                SKIP {current_offset}
                LIMIT {PAGE_SIZE}

                /* Step 3: Collect them into a list for optional match */
                WITH collect(n) AS page_nodes

                /* Step 4: UNWIND and do OPTIONAL MATCH to check inbound edges */
                UNWIND page_nodes AS candidate
                OPTIONAL MATCH (m)-[r]->(candidate)
                WITH candidate, COUNT(m) AS inbound_count

                /* Step 5: Keep only those with zero inbound edges */
                WHERE inbound_count = 0
                RETURN id(candidate) AS no_inbound_id
            $$) AS (no_inbound_id agtype);
        """

        page_result = run_apache_age_query(query_page)

        for row in page_result:
            raw_str = str(row[0]).strip('"')
            no_inbound_ids.append(int(raw_str))

        current_offset += PAGE_SIZE

    name_chunk_size = 500000
    final_named = []

    def chunker(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i:i+size]

    for sub_ids in tqdm(list(chunker(no_inbound_ids, name_chunk_size)), desc="Retrieving names"):
        id_list_str = ", ".join(str(x) for x in sub_ids)
        query_names = f"""
            SELECT node_id, node_name
            FROM cypher('iw_graph', $$
                MATCH (n)
                WHERE id(n) IN [{id_list_str}]
                RETURN id(n) AS node_id, n.name AS node_name
            $$) AS (node_id agtype, node_name agtype);
        """
        sub_result = run_apache_age_query(query_names)

        for row in sub_result:
            raw_id = str(row[0]).strip('"')
            raw_name = str(row[1]).strip('"')
            final_named.append((int(raw_id), raw_name))

    print(f"\nTotal {len(final_named)} such nodes.\n")


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

def task_11():
    query_total = """
        SELECT total_n
        FROM cypher('iw_graph', $$
            MATCH (n:Category)
            RETURN COUNT(n) AS total_n
        $$) AS (total_n agtype);
    """
    result_total = run_apache_age_query(query_total)
    total_nodes = int(str(result_total[0][0]).strip('"'))
    
    PAGE_SIZE = 500000 # best by tests 
    num_pages = ceil(total_nodes / PAGE_SIZE)

    one_child_ids = []
    current_offset = 0

    for _ in tqdm(range(num_pages), desc="Retrieving & checking Category node pages"):
        query_page = f"""
            SELECT node_id
            FROM cypher('iw_graph', $$
                MATCH (n:Category)
                WITH n
                ORDER BY id(n)
                SKIP {current_offset}
                LIMIT {PAGE_SIZE}
                MATCH (n)-[:has]->(c)
                WITH n, COUNT(c) AS num_childs
                WHERE num_childs = 1
                RETURN id(n) AS node_id
            $$) AS (node_id agtype);
        """

        page_result = run_apache_age_query(query_page)

        for row in page_result:
            raw_str = str(row[0]).strip('"')
            one_child_ids.append(int(raw_str))

        current_offset += PAGE_SIZE

    name_chunk_size = 500000
    final_named = []

    def chunker(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i:i+size]

    for sub_ids in tqdm(list(chunker(one_child_ids, name_chunk_size)), desc="Retrieving names"):
        if not sub_ids:
            continue
        id_list_str = ", ".join(str(x) for x in sub_ids)
        query_names = f"""
            SELECT node_id, node_name
            FROM cypher('iw_graph', $$
                MATCH (n:Category)
                WHERE id(n) IN [{id_list_str}]
                RETURN id(n) AS node_id, n.name AS node_name
            $$) AS (node_id agtype, node_name agtype);
        """
        sub_result = run_apache_age_query(query_names)

        for row in sub_result:
            raw_id = str(row[0]).strip('"')
            raw_name = str(row[1]).strip('"')
            final_named.append((int(raw_id), raw_name))

    for nid, nm in final_named:
        print(f" - Node ID={nid}, name={nm}")

    # print(len(final_named))

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

def task_14(start_name, end_name, max_path_length=10, incremental_step=1):
    paths = []
    current_path_length = 1

    while current_path_length <= max_path_length:
        query = f"""
            SELECT p
            FROM cypher('iw_graph', $$
                MATCH p = (startNode)-[*{current_path_length}]->(endNode)
                WHERE startNode.name = '{start_name.replace("'", "\'")}' 
                  AND endNode.name = '{end_name.replace("'", "\'")}'
                RETURN p
            $$) AS (p agtype);
        """

        result = run_apache_age_query(query)

        if result:
            current_paths = [row[0] for row in result]
            num_found = len(current_paths)
            paths.extend(current_paths)

        current_path_length += incremental_step

    if paths:
        print(f"\nTotal paths found from '{start_name}' to '{end_name}': {len(paths)}\n")
        for idx, path in enumerate(paths, 1):
            print(f"Path {idx}: {path}")
    else:
        print(f"\nNo paths found from '{start_name}' to '{end_name}' within path lengths 1 to {max_path_length}.")

    return paths

def task_15(start_name, end_name, max_path_length=10, incremental_step=1):
    paths = []
    current_path_length = 1

    while current_path_length <= max_path_length:
        start_name_escaped = start_name.replace("'", "''")
        end_name_escaped = end_name.replace("'", "''")

        query = f"""
            SELECT p
            FROM cypher('iw_graph', $$
                MATCH p = (startNode)-[*{current_path_length}]->(endNode)
                WHERE startNode.name = '{start_name_escaped}' 
                  AND endNode.name = '{end_name_escaped}'
                RETURN p
            $$) AS (p agtype);
        """

        result = run_apache_age_query(query)

        if result:
            current_paths = [row[0] for row in result]
            paths.extend(current_paths)

        current_path_length += incremental_step

    print(f"\nTotal paths found from '{start_name}' to '{end_name}': {len(paths)}")

    return paths

def task_16(node_name, r):
    # 16. policzy popularność w sąsiedztwie węzła o zadanym promieniu; parametrami są: nazwa
    # węzła oraz promień sąsiedztwa; popularność sąsiedztwa jest sumą popularności danego
    # wązła oraz wszystkich węzłów należących do sąsiedztwa

    # -[*..{r}]->   NIE DZIALA!!!
    if r < 0: raise Exception
    query = f"""
        WITH nodes0 AS (
            SELECT * FROM cypher('iw_graph', $$
                MATCH (n {{name: '{node_name}'}})
                RETURN n.name, n.popularity
            $$) AS result(node_name agtype, popularity float)
        )
        """

    def generation_pattern(n):
        pattern = "-[e1]->"
        for i in range(1, n+1):
            pattern += f"()-[e{i+1}]->"
        return pattern


    for i in range(r):
        path = generation_pattern(i)
        query += f""",
        nodes{i+1} AS (
            SELECT * FROM cypher('iw_graph', $$
                MATCH (n {{name: '{node_name}'}}){path}(neighbor)
                RETURN neighbor.name, neighbor.popularity
            $$) AS result(node_name agtype, popularity float)
        )
        """

    union_queries = "\n        UNION ALL\n        ".join([f"SELECT * FROM nodes{i}" for i in range(r + 1)])

    query += f""",
        combined_nodes AS (
            {union_queries}
        )
        SELECT SUM(popularity) AS total_popularity
        FROM (SELECT DISTINCT node_name, popularity FROM combined_nodes) AS unique_nodes;
    """
    print(query)
    print(run_apache_age_query(query))

def task_17(node_name1, node_name2):
    # 17. policzy popularność na najkrótszej ścieżce między dwoma danymi węzłami, zgodnie ze
    # skierowaniem; popularność na najkrótszej ścieżce jest sumą popularnośi wszystkich węzłów
    # znajdujących się na najkrótszej ścieżce
    query = f"""
        WITH paths_cte AS (
            SELECT * FROM cypher('iw_graph', $$
                MATCH path = (V:Category {{name: '{node_name1}'}})-[*]->(V2:Category {{name: '{node_name2}'}})
                UNWIND nodes(path) AS nodes_on_path
                RETURN nodes_on_path.popularity, length(path)
            $$) AS result(popularity_on_path float, path_len int)
        )
        SELECT popularity_on_path
        FROM paths_cte
        ORDER BY path_len ASC
        LIMIT 1;
    """
    print(query)
    print(run_apache_age_query(query))

def task_18(node_name1, node_name2):
    # 18. znajdzie skierowaną ścieżkę pomiędzy dwoma węzłami o największej popularności spośród
    # wszystkich ścieżek pomiędzy tymi węzłami
    query = f"""
        WITH paths_cte AS (
            SELECT * FROM cypher('iw_graph', $$
                MATCH path = (V:Category {{name: '{node_name1}'}})-[*]->(V2:Category {{name: '{node_name2}'}})
                UNWIND nodes(path) AS nodes_on_path
                RETURN nodes_on_path.popularity, path
            $$) AS result(popularity_on_path float, path agtype)
        )
        SELECT path
        FROM paths_cte
        ORDER BY popularity_on_path DESC
        LIMIT 1;
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
    elif task_number == 14:
        task_14(args[0], args[1])
    elif task_number == 15:
        task_15(args[0], args[1])
    elif task_number == 16:
        task_16(args[0], int(args[1]))
    elif task_number == 17:
        task_17(args[0], args[1])
    elif task_number == 18:
        task_18(args[0], args[1])
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
