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
COMMAND_FILE = "/commands.txt"
LOCAL_COMMAND_FILE = "commands.txt"

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

# Generate the command file
def generate_command_file(popularity_df, taxonomy_df, output_file=LOCAL_COMMAND_FILE):
    print("Generating cypher command file...")
    with open(output_file, "w") as f:
        # Write graph creation commands
        f.write("CREATE EXTENSION IF NOT EXISTS age;\n")
        f.write("LOAD 'age';\n")
        f.write("SET search_path = ag_catalog, \"$user\", public;\n")
        f.write("SELECT * FROM ag_catalog.create_graph('iw_graph');\n")
    
        f.write("BEGIN;\n")
        batch_size = 500
        # Process and write nodes in batches
        for i, batch_start in enumerate(range(0, len(popularity_df), batch_size)):
            batch_end = min(batch_start + batch_size, len(popularity_df))
            batch = popularity_df.iloc[batch_start:batch_end]

            # Begin cypher block
            f.write("SELECT * FROM cypher('iw_graph', $$\n")
            
            for _, row in batch.iterrows():
                f.write(
                    f"  CREATE (:Node {{name: '{row['node_name']}', page_views: {row['page_views']}}})\n"
                )
            
            # End cypher block
            f.write("$$) AS (v agtype);\n")
            if i == 20: # 10k
                break;
        f.write("COMMIT;\n")
        
        # Write commands for creating edges
        # for _, row in taxonomy_df.iterrows():
        #     f.write(
        #         f"SELECT * FROM cypher('iw_graph', $$ MATCH (a:Node {{name: '{row['from']}'}}), (b:Node {{name: '{row['to']}'}}) CREATE (a)-[:RELATED_TO]->(b)$$) AS (v agtype);\n"
        #     )

    print(f"Command file {output_file} generated successfully.")

# Automatically insert the data into the PostgreSQL AGE database
def insert_into_db():
    print("Inserting data into PostgreSQL AGE database...")
    
    # Copy the command file to the Docker container
    subprocess.run(["docker", "cp", LOCAL_COMMAND_FILE, f"{DOCKER_CONTAINER}:{COMMAND_FILE}"], check=True)
    
    # Execute the commands in the container
    subprocess.run(
        ["docker", "exec", "-it", DOCKER_CONTAINER, "psql", "-U", "postgres", "-f", COMMAND_FILE],
        stdout=subprocess.DEVNULL,
        check=True
    )

    print("Data successfully inserted into the PostgreSQL AGE database.")

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
        dataframe[col] = dataframe[col].str.replace("'", "", regex=False)  # Remove single quotes
        dataframe[col] = dataframe[col].str.replace("$", "S", regex=False)  # Replace $ with S
    return dataframe

# Main execution flow
if __name__ == "__main__":
    popularity_df = process_popularity()
    taxonomy_df = process_taxonomy()

    # Clean the data
    popularity_df = clean_data(popularity_df, ["node_name"])
    taxonomy_df = clean_data(taxonomy_df, ["from", "to"])

    count_rows(popularity_df, taxonomy_df)
    
    # Generate the command file
    generate_command_file(popularity_df, taxonomy_df)
    
    # Insert data into the database
    #insert_into_db()
