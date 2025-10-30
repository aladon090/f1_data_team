import pandas as pd
from sqlalchemy import create_engine, text
from google.cloud import bigquery
from google.oauth2 import service_account

class Load:
    def __init__(
        self,
        project_id="f1-data-etl",
        dataset_name="f1_dataset",
        table_name="teams",
        db_path='sqlite:///f1_teams.db',
        gcp_key_path=None
    ):
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.full_table_name = f"{project_id}.{dataset_name}.{table_name}"
        self.engine = create_engine(db_path)

        # Setup BigQuery client
        if gcp_key_path:
            credentials = service_account.Credentials.from_service_account_file(gcp_key_path)
            self.bq_client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            self.bq_client = bigquery.Client(project=project_id)

    # ---------------- SQLite ----------------
    def create_sql_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS f1_teams (
            id INTEGER PRIMARY KEY,
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
        with self.engine.begin() as conn:
            conn.execute(text(query))
        print(" SQLite table created.")

    def load_sql_table(self, df: pd.DataFrame):
        df.to_sql('f1_teams', con=self.engine, if_exists='replace', index=False)
        print("Data loaded into SQLite.")

    # ---------------- BigQuery ----------------
    def load_bigquery(self, df: pd.DataFrame):
        table_id = self.full_table_name

        # Ensure dataset exists
        dataset_ref = self.bq_client.dataset(self.dataset_name)
        try:
            self.bq_client.get_dataset(dataset_ref)
            print(" BigQuery dataset exists.")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.bq_client.create_dataset(dataset)
            print(" BigQuery dataset created.")

        # Load data
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f" Data loaded into BigQuery table `{table_id}`.")

