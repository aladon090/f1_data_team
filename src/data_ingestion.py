import pandas as pd
import requests
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


def ingest_f1_json(API_F1_KEY, F1_URL):
    headers = {
    'X-RapidAPI-Key': API_F1_KEY,
    'X-RapidAPI-Host': 'v1.formula-1.api-sports.io'
    }

    response = requests.get(F1_URL, headers=headers)

    # Check the response status
    if response.status_code == 200:
        team_data = response.json()
        print(json.dumps(team_data, indent=4))
    else:
        raise Exception("Error: {response.status_code} - {response.text}")
    
    return team_data

def transform_to_df(F1_FILE_PATH):
    

    with open(F1_FILE_PATH, 'r') as f:
        data = json.load(f)
    
    # Extracting the list of teams
    data_team = data['response']

    rows = []
    column_names = ['id','name','location','first_entry','Highest_accolades', 'Fastest_lap_time','times_achieved', 'president','director', 'chassis','engine','tyres']

    for team in data_team:
        id = team['id']
        name = team['name']
        location = team['base']
        first_entry = team['first_team_entry']
        Highest_accolades = team["world_championships"]
        fastest_lap_time = team["highest_race_finish"]["position"]
        times_achieved = team["highest_race_finish"]["number"]
        president = team["president"]
        director = team["director"]
        chassis = team["chassis"]
        engine = team["engine"]
        tyres = team["tyres"]

    tuple_teams = (id,name,location,first_entry,Highest_accolades,fastest_lap_time,times_achieved,president,director,chassis,engine,tyres)

    rows.append(tuple_teams)

    df = pd.DataFrame(rows,columns=column_names, index=False)
    
    return df

def df_to_parquet(df):
    # Convert Pandas DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df, preserve_index=False)

    #  Ensure the directory exists
    output_dir = '/workspaces/f1_data_team/data'
    os.makedirs(output_dir, exist_ok=True)

    #  Define the full file path 
    file_path = os.path.join(output_dir, 'f1_teams.parquet')

    #  Write the table to disk
    pq.write_table(
        table,
        file_path,        # The first positional argument should be the destination path
        row_group_size=100
    )
    
    print(f"File successfully saved to: {file_path}")
    


if __name__ == '__main__':
    load_dotenv()
    API_F1_KEY = os.getenv("F1_API_KEY")
    F1_URL = "https://v1.formula-1.api-sports.io/teams"
    
    transform_to_parquet()
