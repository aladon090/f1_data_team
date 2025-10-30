import pandas as pd

class Transform:

    def clean_rows(self, data_team):
        rows = []

        column_names = [
            'id', 'name', 'location', 'first_entry', 'highest_accolades',
            'fastest_lap_time', 'times_achieved', 'president', 'director',
            'chassis', 'engine', 'tyres'
        ]

        for team in data_team:
            rows.append((
                team.get('id'),
                team.get('name'),
                team.get('base'),
                team.get('first_team_entry'),
                team.get('world_championships'),
                team.get('highest_race_finish', {}).get('position'),
                team.get('highest_race_finish', {}).get('number'),
                team.get('president'),
                team.get('director'),
                team.get('chassis'),
                team.get('engine'),
                team.get('tyres')
            ))

        return pd.DataFrame(rows, columns=column_names)

    def clean_unknowns(self, df):
        df = df.copy()

        # Fill missing values
        fill_str = ['name', 'location', 'president', 'director', 'chassis', 'engine', 'tyres']
        fill_num = ['first_entry', 'highest_accolades', 'fastest_lap_time', 'times_achieved']

        df[fill_str] = df[fill_str].fillna("Unknown")
        df[fill_num] = df[fill_num].fillna(0).astype(int)

        # Remove whitespace and standardize formatting
        df[fill_str] = df[fill_str].apply(lambda col: col.astype(str).str.strip())
        df['name'] = df['name'].str.upper()
        df[['location', 'chassis', 'tyres']] = df[['location', 'chassis', 'tyres']].apply(
            lambda col: col.str.title()
        )

        return df


