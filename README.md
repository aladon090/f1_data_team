# F1 Team Performance Analytics Pipeline

## Project Overview
An end-to-end data engineering pipeline designed to analyze historical Formula 1 data, focusing on engine manufacturer dominance, team sustainability, and performance categories.  

**Primary Goal:**  
Determine the best-performing combinations of engine, team, and tire manufacturer across F1 history by automating the flow from raw API data to an analytics-ready layer.

**Key Business Questions:**  
- Which engine manufacturers have the highest success-to-entry ratio?  
- Which teams maintain consistent momentum across different technical eras?  
- Which tire suppliers correlate with championship-winning performance?  

---

## Technical Architecture
The pipeline follows a modern data stack approach, ensuring scalability, reliability, and maintainability.  

**1. Extraction**  
- Python scripts fetch real-time and historical F1 data from the F1 API.  

**2. Orchestration**  
- Apache Airflow manages task dependencies, retries, and scheduling (weekly ingestion).  

**3. Data Lake**  
- Raw JSON data is converted to Parquet and stored in **Google Cloud Storage (GCS)** for efficient, scalable storage.  

**4. Data Warehouse**  
- **BigQuery** stores structured data for high-performance analytics.  

**5. Transformation**  
- **dbt (Data Build Tool)** transforms raw data into cleaned and tested fact tables.  

---

## Key Data Marts (dbt Models)
- `stg_f1_teams` – Cleans raw JSON API data into structured staging tables.  
- `int_f1_team_success` – Calculates weighted success scores for teams.  
- `fct_engine_dominance` – Measures wins/podiums by engine manufacturer.  
- `fct_team_momentum` – Analyzes recent team performance trends.  

---

## Pipeline Architecture
![Pipeline Architecture](./d381b725-89cd-401f-827d-32452b502f44.png)

---

## Final Analytics Breakdown
The dashboard illustrates the final output of the pipeline, displaying engine success distribution, team performance bucketing, and supplier dominance.  

![Dashboard](https://github.com/user-attachments/assets/4dec9f94-1fd6-45f4-a020-114ae316d501)

---

## Setup & Execution

**Trigger Pipeline via Airflow:**  
```bash
airflow dags trigger f1_pipeline
