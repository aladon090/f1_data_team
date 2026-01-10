import pandas as pd
import requests
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# -------------------------------
# Function: ingest_f1_json
# Purpose: Fetch F1 team data from the API
# -------------------------------
def ingest_f1_json(API_F1_KEY, F1_URL):
    """
    Calls the F1 API and returns JSON data.
    
    Args:
        API_F1_KEY (str): API key for authentication
        F1_URL (str): Endpoint URL for F1 teams API
        
    Returns:
        dict: JSON response from the API
    """
    # Set headers for API-Sports direct API
    headers = {
        "x-apisports-key": API_F1_KEY
    }

    # Make the GET request
    response = requests.get(F1_URL, headers=headers, timeout=30)  # timeout prevents hanging

    # Raise Exception if request fails
    if response.status_code != 200:
        raise Exception(
            f"F1 API call failed | "
            f"status={response.status_code} | "
            f"response={response.text}"
        )

    # Return parsed JSON
    return response.json()


# -------------------------------
# Function: transform_to_df
# Purpose: Convert raw JSON into a structured Pandas DataFrame
# -------------------------------
def transform_to_df(F1_FILE_PATH):
    """
    Reads JSON file and transforms it into a DataFrame with one row per team.

    Args:
        F1_FILE_PATH (str): Path to local JSON file with F1 data
    
    Returns:
        pd.DataFrame: DataFrame containing all teams
    """
    # Load the JSON file
    with open(F1_FILE_PATH, 'r') as f:
        data = json.load(f)

    rows = []

    # Loop through all teams in the response
    for team in data.get("response", []):
        # Append each team as a tuple
        rows.append((
            team['id'],
            team['name'],
            team['base'],                       # Location/base of the team
            team['first_team_entry'],            # First year in F1
            team['world_championships'],         # Total championships won
            team['highest_race_finish']['position'],  # Best race finish position
            team['highest_race_finish']['number'],    # Times best finish was achieved
            team['president'],
            team['director'],
            team['chassis'],
            team['engine'],
            team['tyres']
        ))

    # Define DataFrame columns
    columns = [
        'id','name','location','first_entry',
        'world_championships','highest_finish_position',
        'times_achieved','president','director',
        'chassis','engine','tyres'
    ]

    # Create and return DataFrame
    return pd.DataFrame(rows, columns=columns)


# -------------------------------
# Function: df_to_parquet
# Purpose: Save a Pandas DataFrame as a Parquet file using PyArrow
# -------------------------------
def df_to_parquet(df):
    """
    Converts a DataFrame to Parquet and saves it locally.
    
    Args:
        df (pd.DataFrame): DataFrame to save
    
    Returns:
        str: Full file path of the saved Parquet file
    """
    # Ensure the output directory exists
    output_dir = '/workspaces/f1_data_team/data'
    os.makedirs(output_dir, exist_ok=True)

    # Set full Parquet file path
    file_path = os.path.join(output_dir, 'f1_teams.parquet')

    # Convert DataFrame to PyArrow Table (efficient columnar format)
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Write the table to disk in Parquet format
    pq.write_table(table, file_path, row_group_size=100)  # row_group_size improves read performance

    # Return path to Parquet file (used by Airflow XCom or next steps)
    return file_path


if __name__ == "__main__":
    load_dotenv()
    API_F1_KEY = os.getenv("F1_API_KEY")
    F1_URL = "https://v1.formula-1.api-sports.io/teams"

    TEMP_JSON_PATHWAY = "f1_teams_raw.json"

    try:
        print("Starting F1 Team Data Ingestion...")
        
        # FIX: Changed API_KEY to API_F1_KEY and API_URL to F1_URL
        raw_data = ingest_f1_json(API_F1_KEY=API_F1_KEY, F1_URL=F1_URL)
        
        with open(TEMP_JSON_PATHWAY, 'w') as f:
            json.dump(raw_data, f)
        
        # TRANSFORM: Convert JSON to DataFrame
        print("Transforming data to DataFrame...")
        teams_df = transform_to_df(TEMP_JSON_PATHWAY)
        
        # LOAD: Save to Parquet
        print(f"Saving {len(teams_df)} teams to Parquet...")
        parquet_path = df_to_parquet(teams_df)
        
        print(f"Success! Data saved to: {parquet_path}")

    except Exception as e:
        print(f"Pipeline Failed: {e}")
    

    
    
    
