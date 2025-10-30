from extract.Extract import Extract
from transform.Transform import Transform
from load.Load import Load
from visualise.visualise import Visualization

import pandas as pd
from sqlalchemy import create_engine
import sys

def main():
    try:
        # -------------------- Extract --------------------
        extractor = Extract()
        team_json = extractor.extract()
        if not team_json:
            raise ValueError("Extract returned no data.")
        print("Extract step completed successfully.")

        # -------------------- Transform --------------------
        transformer = Transform()
        df = transformer.clean_rows(team_json)
        clean_df = transformer.clean_unknowns(df)
        if clean_df.empty:
            raise ValueError("Transform step produced an empty DataFrame.")
        print("Transform step completed successfully.")

        # -------------------- Load --------------------
        # Path to service account JSON
        key_path = r"C:\Users\akillas\f1_data_team\credentials\big_query.json"

        loader = Load(gcp_key_path=key_path)

        # SQLite
        loader.create_sql_table()
        loader.load_sql_table(clean_df)
        print(" Load step completed successfully.")

        # BigQuery
        loader.load_bigquery(clean_df)

        print("BigQuery load completed ")
        print("Load step completed successfully.")

        # -------------------- Verify Load --------------------
        try:
            engine = create_engine('sqlite:///data/f1_teams.db')
            df_from_db = pd.read_sql("SELECT * FROM f1_teams", engine)
            print("Data loaded into SQL database:")
            print(df_from_db.head())
        except Exception as sql_err:
            print(f"Failed to read from database: {sql_err}")

        # -------------------Visualise -----------------
        try:
            visualise = Visualization(clean_df)
            plot = visualise.plot_championships()
        except Exception as visualise_err:
            print(f"Failed to plot championships: {visualise_err}")




    except ValueError as ve:
        print(f"Validation Error: {ve}")

    except ConnectionError:
        print("API connection failed. Please check network or API key.")

    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

    else:
        print("ETL Pipeline Completed Successfully.")

    finally:
        print("Script finished running.")

if __name__ == "__main__":
    main()

