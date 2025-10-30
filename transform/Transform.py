import pandas as pd

class Transform:
    def clean_rows(self, data_team):
        rows = []

        column_names = [
            'id', 'name', 'location', 'first_entry', 'highest_accolades', 'fastest_lap_time',
            'times_achieved', 'president', 'director', 'chassis', 'engine', 'tyres'
        ]

        for team in data_team:
            tuple_teams = (
                team.get('id'),
                team.get('name'),
                team.get('base'),
                team.get('first_team_entry'),
                team.get("world_championships"),
                team.get("highest_race_finish", {}).get("position"),
                team.get("highest_race_finish", {}).get("number"),
                team.get("president"),
                team.get("director"),
                team.get("chassis"),
                team.get("engine"),
                team.get("tyres")
            )
            rows.append(tuple_teams)

        df = pd.DataFrame(rows, columns=column_names)
        return df

    def clean_unknowns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean missing or inconsistent values in the DataFrame.
        """
        # Fill missing values
        df.fillna({
            'location': 'Unknown',
            'chassis': 'Unknown',
            'first_entry': 0,
            'highest_accolades': 0,
            'fastest_lap_time': 0,
            'times_achieved': 0
        }, inplace=True)

        # Convert numeric columns to integers
        num_cols = ['first_entry', 'highest_accolades', 'fastest_lap_time', 'times_achieved']
        df[num_cols] = df[num_cols].astype(int)

        # Standardize column names
        df.columns = (
            df.columns.str.strip()
                      .str.lower()
                      .str.replace(' ', '_')
                      .str.replace(r'[^\w_]', '', regex=True)
        )

        # Strip text columns
        text_cols = ['name', 'location', 'president', 'director', 'chassis', 'engine', 'tyres']
        df[text_cols] = df[text_cols].apply(lambda x: x.astype(str).str.strip())

        # Apply consistent casing
        df['name'] = df['name'].str.upper()
        df[['location', 'chassis', 'tyres']] = df[['location', 'chassis', 'tyres']].apply(lambda x: x.str.title())

        return df

