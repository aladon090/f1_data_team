import os
import requests
import json
import pandas as pd
from dotenv import load_dotenv


class Extract:
    def __init__(self, file_path=None):
        """
        Initialize the Extract class.

        Parameters:
        file_path (str, optional): Path to a CSV file to extract data from. Defaults to None.
        """
        self.file_path = file_path

    def extract(self,URL=None):
        """
        Extract F1 team data from API or CSV (if provided) and return a pandas DataFrame.
        """
        # If a CSV file path is provided, extract from CSV
        if self.file_path:
            try:
                df = pd.read_csv(self.file_path)
                print(f"Data successfully extracted from {self.file_path}")
                return df
            except FileNotFoundError:
                print(f"File not found: {self.file_path}")
                return None

        # Otherwise, extract from the API
        load_dotenv()  # Load environment variables
        api_key = os.getenv('api_key')

        if URL is None:
            URL = 'https://v1.formula-1.api-sports.io/teams'


        headers = {
            'X-RapidAPI-Key': api_key,
            'X-RapidAPI-Host': 'v1.formula-1.api-sports.io'
        }

        response = requests.get(URL, headers=headers)

        if response.status_code == 200:
            team_data = response.json()
            print("Data successfully extracted from API:")
            print(json.dumps(team_data, indent=4))  # Pretty print JSON response

            # Convert API response to pandas DataFrame
            teams = team_data.get("response", [])
            df = pd.json_normalize(teams)
            return df
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

