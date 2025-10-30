import pandas as pd
from sqlalchemy import create_engine, text

class Load:
    def __init__(self, db_path='sqlite:///f1_teams.db'):
        self.engine = create_engine(db_path)

    def create_sql_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS f1_teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            location TEXT,
            first_entry INTEGER,
            highest_accolades INTEGER,
            fastest_lap_time INTEGER,
            times_achieved INTEGER,
            president TEXT,
            director TEXT,
            chassis TEXT,
            engine TEXT,
            tyres TEXT
        );
        """
        with self.engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print(" Table 'f1_teams' created successfully (or already exists).")

    def load_sql_table(self, df: pd.DataFrame):
        df.to_sql('f1_teams', con=self.engine, if_exists='replace', index=False)
        print(" Data loaded into f1_teams table.")
