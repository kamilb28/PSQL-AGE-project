import pandas as pd
import gzip
import psycopg2
import os
import subprocess

# Paths to the data files
POPULARITY_PATH = "popularity_iw.csv.gz"
TAXONOMY_PATH = "taxonomy_iw.csv.gz"

# Docker container and file paths
DOCKER_CONTAINER = "age-container"

# Process the popularity data
def process_popularity():
    pickle_path = "processed_popularity.pkl"
    if os.path.exists(pickle_path):
        print("Loading popularity data from pickle...")
        df = pd.read_pickle(pickle_path)
    else:
        print("Processing popularity data...")
        df = pd.read_csv(POPULARITY_PATH, compression='gzip', header=None, names=["node_name", "page_views"])
        df.to_pickle(pickle_path)
    return df

# Process the taxonomy data
def process_taxonomy():
    pickle_path = "processed_taxonomy.pkl"
    if os.path.exists(pickle_path):
        print("Loading taxonomy data from pickle...")
        df = pd.read_pickle(pickle_path)
    else:
        print("Processing taxonomy data...")
        graph_tuples = []
        with gzip.open(TAXONOMY_PATH, 'rt', encoding='utf-8') as f:
            for line in f:
                comma_index = get_comma_index_not_in_quotes(line)
                if comma_index != -1:
                    parent = line[:comma_index]
                    child = line[comma_index + 1:].strip()
                    graph_tuples.append((parent, child))
        df = pd.DataFrame(graph_tuples, columns=['from', 'to'])
        df['from'] = df['from'].map(remove_quotes_from_string)
        df['to'] = df['to'].map(remove_quotes_from_string)
        df.to_pickle(pickle_path)
    return df

# Helper functions
def get_comma_index_not_in_quotes(line):
    in_quotes = False
    for i, c in enumerate(line):
        if c == '"':
            in_quotes = not in_quotes
        if c == ',' and not in_quotes:
            return i
    return -1

def remove_quotes_from_string(s):
    start_index = 1 if s[0] == '"' else 0
    end_index = -1 if s[-1] == '"' else len(s)
    return s[start_index:end_index]


# Count rows in both DataFrames
def count_rows(popularity_df, taxonomy_df):
    print(f"Number of rows in popularity DataFrame: {len(popularity_df)}")
    print(f"Number of rows in taxonomy DataFrame: {len(taxonomy_df)}")

def clean_data(dataframe, columns):
    for col in columns:
        dataframe[col] = dataframe[col].str.replace("'", "-", regex=False)  # Remove single quotes
        dataframe[col] = dataframe[col].str.replace("$", "S", regex=False)  # Replace $ with S
    return dataframe




def make_age_compatable_df(taxonomy_df):
    # Step 1: Create a unique list of all nodes from `from` and `to` columns
    all_nodes = pd.unique(taxonomy_df[['from', 'to']].values.ravel())

    # Step 2: Create a mapping of node names to IDs
    node_mapping = {node: idx for idx, node in enumerate(all_nodes, start=1)}

    # Step 3: Create nodes_df with ID and Name
    nodes_df = pd.DataFrame({
        'id': [node_mapping[node] for node in all_nodes],
        'name': all_nodes
    })

    # Step 4: Create edges_df
    edges_df = taxonomy_df.copy()
    edges_df['start_id'] = edges_df['from'].map(node_mapping)
    edges_df['end_id'] = edges_df['to'].map(node_mapping)
    edges_df['start_vertex_type'] = 'Category'
    edges_df['end_vertex_type'] = 'Category'

    # Keep only the required columns
    edges_df = edges_df[['start_id', 'start_vertex_type', 'end_id', 'end_vertex_type']]


    # Merge popularity data into nodes_df
    nodes_df = nodes_df.merge(
        popularity_df,
        how='left',  # Use left join to keep all nodes even if they don't have popularity data
        left_on='name',  # Match `name` in nodes_df
        right_on='node_name'  # Match `node_name` in popularity_df
    )

    # Drop the redundant node_name column (optional)
    nodes_df = nodes_df.drop(columns=['node_name'])

    # Rename page_views to popularity for clarity
    nodes_df = nodes_df.rename(columns={'page_views': 'popularity'})

    # Fill NaN values in popularity with 0
    nodes_df['popularity'] = nodes_df['popularity'].fillna(0)

    print(edges_df.head())
    print(nodes_df.head())

    print(f"Number of rows in edges_df DataFrame: {len(edges_df)}")
    print(f"Number of rows in nodes_df DataFrame: {len(nodes_df)}")

    # Export nodes_df to a CSV file
    nodes_df.to_csv('nodes.csv', index=False)

    # Export edges_df to a CSV file
    edges_df.to_csv('edges.csv', index=False)

    print("DataFrames have been exported as 'nodes.csv' and 'edges.csv'.")

def copy_data_into_container():
    docker_container = "age-container"
    docker_target_dir = "/age/regress/age_load/data/project"

    # Create the target directory inside the Docker container
    subprocess.run(
        ["docker", "exec", docker_container, "mkdir", "-p", docker_target_dir],
        check=True
    )
    print(f"Created directory {docker_target_dir} in Docker container {docker_container}.")

    # Copy the CSV files into the Docker container
    subprocess.run(
        ["docker", "cp", "nodes.csv", f"{docker_container}:{docker_target_dir}/nodes.csv"],
        check=True
    )
    subprocess.run(
        ["docker", "cp", "edges.csv", f"{docker_container}:{docker_target_dir}/edges.csv"],
        check=True
    )

    print(f"Copied nodes.csv and edges.csv to {docker_target_dir} in Docker container {docker_container}.")

def insert_data_into_db():
    db_params = {
        "dbname": os.getenv("DB_NAME", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "root"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }    
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cursor = conn.cursor()

        # Execute SQL commands
        cursor.execute("LOAD 'age';")
        cursor.execute("SET search_path TO ag_catalog;")
        cursor.execute("SELECT create_graph('iw_graph');")
        cursor.execute("SELECT create_vlabel('iw_graph', 'Category');")
        cursor.execute("""
            SELECT load_labels_from_file(
                'iw_graph',
                'Category',
                '/age/regress/age_load/data/project/nodes.csv'
            );
        """)

        print("NODES INSERTED SECCESSFULLY.")
    except Exception as e:
        print(f"An error occurred while inserting nodes: {e}")
    finally:
        # Close the connection
        if conn:
            cursor.close()
            conn.close()


    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cursor = conn.cursor()

        # Execute SQL commands
        cursor.execute("LOAD 'age';")
        cursor.execute("SET search_path TO ag_catalog;")
        cursor.execute("SELECT create_elabel('iw_graph', 'has');")
        cursor.execute("""
            SELECT load_edges_from_file(
                'iw_graph',
                'has',
                '/age/regress/age_load/data/project/edges.csv'
            );
        """)

        print("EDGES INSERTED SECCESSFULLY.")
    except Exception as e:
        print(f"An error occurred while inserting edges: {e}")
    finally:
        # Close the connection
        if conn:
            cursor.close()
            conn.close()

# Main execution flow
if __name__ == "__main__":
    popularity_df = process_popularity()
    taxonomy_df = process_taxonomy()

    # Clean the data
    popularity_df = clean_data(popularity_df, ["node_name"])
    taxonomy_df = clean_data(taxonomy_df, ["from", "to"])

    count_rows(popularity_df, taxonomy_df)

    make_age_compatable_df(taxonomy_df)

#    copy_data_into_container()

    insert_data_into_db()
    
