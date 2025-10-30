🏎️ F1 Teams ETL Pipeline with BigQuery & Apache Airflow
 Project Overview
This project showcases a complete ETL (Extract, Transform, Load) pipeline for Formula 1 team data, orchestrated using Apache Airflow and integrated with Google BigQuery for scalable storage and analysis. The pipeline automates the ingestion of raw data, applies transformations for consistency and quality, and loads the final dataset into BigQuery for downstream analytics and visualization.
As a Formula 1 enthusiast, I’m drawn to the sport’s fusion of cutting-edge engineering, strategic depth, and team dynamics. Each team’s legacy, performance metrics, and technical evolution offer rich data for exploration and storytelling.

ETL Pipeline Stages
1. Extract

Source: API-SPORTS F1 API or CSV files
Details: Retrieves official team data including names, locations, championship history, and technical specs.

2. Transform

Cleaning & Standardization:

Fill missing values (e.g., location, chassis)
Normalize column names (lowercase, underscores)
Convert numeric fields to consistent types
Format text fields (e.g., uppercase team names)



3. Load

Target: Google BigQuery
Benefits:

Scalable, fast querying
Integration with BI tools (e.g., Looker, Data Studio)
Supports advanced analytics and ML workflows



4. Orchestration

Tool: Apache Airflow
Purpose:

Automates daily/weekly ETL runs
Monitors task execution and failures
Enables modular DAG design for future expansion
