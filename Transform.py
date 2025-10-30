import pandas as pd

class Transform:
    def __init__(self, default_location='Unknown', default_chassis='Unknown'):
        """
        Initialize the Transform class.

        Parameters:
        default_location (str): Default value for missing location.
        default_chassis (str): Default value for missing chassis.
        """
        self.default_location = default_location
        self.default_chassis = default_chassis

    def transform(self, team_data):
        """
        Main method to transform raw F1 team JSON into a cleaned DataFrame.

        Parameters:
        team_data (dict): JSON response from F1 API.

        Returns:
        pd.DataFrame: Cleaned and normalized team data.
        """
        rows = self._extract_rows(team_data)
        df = pd.DataFrame(rows)
        df = self._clean_numeric(df)
        df = self._clean_text(df)
        df = self._standardize_columns(df)
        return df

    def _extract_rows(self, team_data):
        """Extract relevant fields from JSON and return a list of dictionaries."""
        teams = team_data.get('response', [])
        rows = []
        for team in teams:
            rows.append({
                'id': team.get('id'),
                'name': team.get('name'),
                'location': team.get('base'),
                'first_entry': team.get('first_team_entry'),
                'highest_accolades': team.get('world_championships'),
                'fastest_lap_time': team.get('highest_race_finish', {}).get('position'),
                'times_achieved': team.get('highest_race_finish', {}).get('number'),
                'president': team.get('president'),
                'director': team.get('director'),
                'chassis': team.get('chassis'),
                'engine': team.get('engine'),
                'tyres': team.get('tyres')
            })
        return rows

    def _clean_numeric(self, df):
        """Fill missing numeric columns and convert to integers."""
        numeric_cols = ['first_entry', 'highest_accolades', 'fastest_lap_time', 'times_achieved']
        for col in numeric_cols:
            df[col] = df[col].fillna(0).astype(int)
        return df

    def _clean_text(self, df):
        """Fill missing text columns and standardize string formatting."""
        text_cols = ['name', 'location', 'president', 'director', 'chassis', 'engine', 'tyres']

        # Fill missing values
        df['location'] = df['location'].fillna(self.default_location)
        df['chassis'] = df['chassis'].fillna(self.default_chassis)
        for col in text_cols:
            df[col] = df[col].fillna('Unknown').astype(str).str.strip()

        # Standardize capitalization
        df['name'] = df['name'].str.upper()
        df['location'] = df['location'].str.title()
        df['chassis'] = df['chassis'].str.title()
        df['tyres'] = df['tyres'].str.title()
        return df

    def _standardize_columns(self, df):
        """Normalize column names to snake_case and remove special characters."""
        df.columns = (
            df.columns.str.strip()
                      .str.lower()
                      .str.replace(' ', '_')
                      .str.replace(r'[^\w_]', '', regex=True)
        )
        return df

