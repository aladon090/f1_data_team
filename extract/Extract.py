import os
import requests
from dotenv import load_dotenv

class Extract:
    def __init__(self, file_path=None):
        """
        Initialize the Extract class.
        Parameters:
        file_path (str, optional): Path to a CSV file to extract data from. Defaults to None.
        """
        self.file_path = file_path

    def extract(self, URL=None):
        """
        Extract F1 team data from API or CSV (if provided) and return JSON data.
        """
        # If a CSV file path is provided, extract from CSV
        if self.file_path:
            try:
                import pandas as pd
                df = pd.read_csv(self.file_path)
                return df.to_dict(orient='records')  # Convert CSV to JSON
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
            team_data = response.json()  # This is already JSON
            return team_data.get("response", [])  # Return only the relevant part
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None


