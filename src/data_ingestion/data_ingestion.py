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








if __name__ == '__main__':
    load_dotenv()
    API_F1_KEY = os.getenv("F1_API_KEY")
    F1_URL = "https://v1.formula-1.api-sports.io/teams"
    print(API_F1_KEY)
