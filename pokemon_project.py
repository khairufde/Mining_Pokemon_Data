import sqlite3
import pandas as pd
import requests
import time
from datetime import datetime 
import os

def log_progress(message):
    global output_path
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open(f'{output_path}/pokemon_project_log.txt', 'a') as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract_pokemon_data(pokemon_url, start_id, end_id):
    pokemon_list = []
    pokemon_type_list = []
    move_id_list = []
    
    for id in range(start_id, end_id + 1):
        url = pokemon_url + str(id)
        poke_request = requests.get(url)                                                             
        poke = poke_request.json()   

        poke_dict = {
                    "pokemon_id": id,
                    "pokemon_name": poke["name"],
                    "weight": poke["weight"],
                    "height": poke["height"]
                    }
        pokemon_type_dict = {
                    "pokemon_id": id,
                    "type_1_id": int(poke["types"][0]["type"]["url"].split("/")[-2]) if len(poke["types"]) > 0 else None,
                    "type_1": poke["types"][0]["type"]["name"] if len(poke["types"]) > 0 else None,
                    "type_2_id": int(poke["types"][1]["type"]["url"].split("/")[-2]) if len(poke["types"]) > 1 else None,
                    "type_2": poke["types"][1]["type"]["name"] if len(poke["types"]) > 1 else None
                    }

        for move in poke["moves"]:
            move_url = move["move"]["url"]
            move_id = int(move_url.split("/")[-2])
            move_id_list.append(move_id)
            time.sleep(0.2)

        pokemon_type_list.append(pokemon_type_dict)
        pokemon_list.append(poke_dict)

        time.sleep(0.2)

    pokemon_df = pd.DataFrame(pokemon_list)
    pokemon_type_df = pd.DataFrame(pokemon_type_list)

    move_id_list = list(set(move_id_list))
    move_id_list.sort()

    print(f'Number of values in pokemon : {len(pokemon_list)}')
    print(f'Number of values in pokemon type : {len(pokemon_type_list)}')
    print(f'Number of values in move id : {len(move_id_list)}')

    epd_log = f'Pokemon data successfully extracted'
    print(epd_log)
    log_progress(epd_log)

    return pokemon_df, pokemon_type_df, move_id_list

def extract_move_data(move_url, move_id_list):

    pokemon_move_list = []

    for id in move_id_list:
        url = move_url + str(id)
        try:
            poke_request = requests.get(url)
            poke_request.raise_for_status()
            poke = poke_request.json()
            
            for pokemon_data in poke["learned_by_pokemon"]:
                pokemon_url = pokemon_data["url"]
                pokemon_id = int(pokemon_url.split("/")[-2])
                
                if pokemon_id in pokemon_df["pokemon_id"].values:
                    move_dict = {
                        "pokemon_id": pokemon_id,
                        "move_id": id,
                        "move": poke["name"],
                        "accuracy": poke.get("accuracy"),
                        "power": poke.get("power")
                    }
                    pokemon_move_list.append(move_dict)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for move ID {id}: {e}")

    pokemon_move_df = pd.DataFrame(pokemon_move_list)

    print(f'Number of values in pokemon move : {len(pokemon_move_list)}')

    emd_log = f'Move data successfully extracted'
    print(emd_log)
    log_progress(emd_log)

    return pokemon_move_df

def transform_data(pokemon_df, pokemon_type_df, pokemon_move_df):

    pokemon_df["pokemon_id"] = pokemon_df["pokemon_id"].astype("Int64")
    pokemon_df["weight"] = pokemon_df["weight"].astype("Int64")
    pokemon_df["height"] = pokemon_df["height"].astype("Int64")

    pokemon_type_df["pokemon_id"] = pokemon_type_df["pokemon_id"].astype("Int64")
    pokemon_type_df["type_1_id"] = pokemon_type_df["type_1_id"].fillna(0).astype("Int64")
    pokemon_type_df["type_2_id"] = pokemon_type_df["type_2_id"].fillna(0).astype("Int64")

    type_1_df = pokemon_type_df[["type_1_id", "type_1"]].rename(columns={"type_1_id": "type_id", "type_1": "type"})
    type_2_df = pokemon_type_df[["type_2_id", "type_2"]].rename(columns={"type_2_id": "type_id", "type_2": "type"})
    type_df = pd.concat([type_1_df, type_2_df]).drop_duplicates().reset_index(drop=True)

    pokemon_type_df = pokemon_type_df.drop(columns=["type_1", "type_2"])

    move_df = pokemon_move_df[["move_id", "move", "accuracy", "power"]]
    move_df = move_df.drop_duplicates().reset_index(drop=True)

    move_df["accuracy"] = move_df["accuracy"].fillna(0).astype("Int64")
    move_df["power"] = move_df["power"].fillna(0).astype("Int64")

    pokemon_move_df = pokemon_move_df.drop(columns=["move", "accuracy", "power"])

    td_log = 'Data successfully transformed'
    print(td_log)
    log_progress(td_log)

    return pokemon_df, pokemon_type_df, type_df, pokemon_move_df, move_df

def load_to_csv(pokemon_df, pokemon_type_df, type_df, pokemon_move_df, move_df, csv_path):

    os.makedirs(csv_path, exist_ok=True)

    pokemon_df.to_csv(f'{csv_path}/pokemon.csv', index=False)
    pokemon_type_df.to_csv(f'{csv_path}/pokemon_type.csv', index=False)
    type_df.to_csv(f'{csv_path}/type.csv', index=False)
    pokemon_move_df.to_csv(f'{csv_path}/pokemon_move.csv', index=False)
    move_df.to_csv(f'{csv_path}/move.csv', index=False)

    ltcsv_log = 'Data loaded successfully to csv'
    print(ltcsv_log)
    log_progress(ltcsv_log)

def create_table(sql_connection):
    cursor = sql_connection.cursor()

    # Create the tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pokemon (
        pokemon_id INTEGER PRIMARY KEY NOT NULL,
        pokemon_name VARCHAR(20) NOT NULL,
        weight INTEGER NOT NULL,
        height INTEGER NOT NULL
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pokemon_type (
        pokemon_id INTEGER PRIMARY KEY NOT NULL,
        type_1_id INTEGER NOT NULL,
        type_2_id INTEGER NOT NULL,
        FOREIGN KEY (pokemon_id) REFERENCES pokemon (pokemon_id),
        FOREIGN KEY (type_1_id) REFERENCES type (type_id),
        FOREIGN KEY (type_2_id) REFERENCES type (type_id)
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS type (
        type_id INTEGER PRIMARY KEY NOT NULL,
        type VARCHAR(20)
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pokemon_move (
        pokemon_id INTEGER PRIMARY KEY NOT NULL,
        move_id INTEGER NOT NULL,
        FOREIGN KEY (pokemon_id) REFERENCES pokemon (pokemon_id),
        FOREIGN KEY (move_id) REFERENCES move (move_id)
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS move (
        move_id INTEGER PRIMARY KEY,
        move VARCHAR(20) NOT NULL,
        accuracy INTEGER,
        power INTEGER
    );
    ''')

    sql_connection.commit()

    ct_log = 'Tables created successfully'
    print(ct_log)
    log_progress(ct_log)

def load_to_db(pokemon_df, pokemon_type_df, type_df, pokemon_move_df, move_df, conn):

    pokemon_df.to_sql("pokemon", conn, if_exists="replace", index=False)
    pokemon_type_df.to_sql("pokemon_type", conn, if_exists="replace", index=False)
    type_df.to_sql("type", conn, if_exists="replace", index=False)
    pokemon_move_df.to_sql("pokemon_move", conn, if_exists="replace", index=False)
    move_df.to_sql("move", conn, if_exists="replace", index=False)

    ltdb_log = 'Data loaded successfully into the SQLite database'
    print(ltdb_log)
    log_progress(ltdb_log)

def run_query(query_statement, sql_connection):

    rq_log = 'Running query'
    log_progress(rq_log)
    print(query_statement)

    query_output = pd.read_sql_query(query_statement, sql_connection)

    print(query_output)

pokemon_url = "https://pokeapi.co/api/v2/pokemon/"
move_url = "https://pokeapi.co/api/v2/move/"
db_name = 'pokemon.db'
output_path = './pokemon_project'
os.makedirs(output_path, exist_ok=True)
db_path = os.path.join(output_path, db_name)
start_id = 1
end_id = 15

start_time = time.time()

# Extract the Pokemon data and get the move ID list and pass move_id_list to extract_move_data()
pokemon_df, pokemon_type_df, move_id_list = extract_pokemon_data(pokemon_url, start_id, end_id)
pokemon_move_df = extract_move_data(move_url, move_id_list)

# Transform the data
pokemon_df, pokemon_type_df, type_df, pokemon_move_df, move_df = transform_data(pokemon_df, pokemon_type_df, pokemon_move_df)

# Load the data to CSV
load_to_csv(pokemon_df, pokemon_type_df, type_df, pokemon_move_df, move_df, output_path)

# Access database
sql_connection = sqlite3.connect(db_path)

create_table(sql_connection)

# Load the data into Database
load_to_db(pokemon_df, pokemon_type_df, type_df, pokemon_move_df, move_df, sql_connection)

# Query the database
query = '''
    SELECT 
        * FROM pokemon LIMIT 5;
'''
run_query(query, sql_connection)

# What is the average weight of the pokemon by Pokemon type?
query = '''
    SELECT 
        t1.type AS type_1, 
        t2.type AS type_2,
        AVG(p.weight) AS average_weight_by_type
    FROM pokemon p
    JOIN pokemon_type pt ON p.pokemon_id = pt.pokemon_id
    LEFT JOIN type t1 ON pt.type_1_id = t1.type_id
    LEFT JOIN type t2 ON pt.type_2_id = t2.type_id
    GROUP BY t1.type, t2.type;
'''
run_query(query, sql_connection)

# List the highest accuracy move by Pokemon type!
query = '''
    SELECT
        t1.type AS type_1,
        t2.type AS type_2,
        MAX(m.accuracy) AS highest_accuracy_by_type
    FROM pokemon p
    JOIN pokemon_type pt ON p.pokemon_id = pt.pokemon_id
    LEFT JOIN type t1 ON pt.type_1_id = t1.type_id
    LEFT JOIN type t2 ON pt.type_2_id = t2.type_id
    JOIN pokemon_move pm ON p.pokemon_id = pm.pokemon_id
    JOIN move m ON pm.move_id = m.move_id

    GROUP BY t1.type, t2.type;
'''

run_query(query, sql_connection)

# Count the number of moves by Pokemon and order from greatest to least!
query = '''
    SELECT 
        p.pokemon_name, 
        count(m.move) AS number_of_move_by_pokemon
    FROM pokemon p
    JOIN pokemon_move pm ON p.pokemon_id = pm.pokemon_id
    JOIN move m ON pm.move_id = m.move_id
    GROUP BY p.pokemon_name
    ORDER BY number_of_move_by_pokemon DESC
    ;
'''

run_query(query, sql_connection)

sql_connection.close()

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time taken to run the code: {elapsed_time:.2f} seconds")