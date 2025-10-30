F1 Teams ETL Pipeline
Project Overview

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline using Formula 1 team data. The pipeline extracts raw data from an API or CSV, transforms it for consistency and completeness, and loads it into a SQLite database. This setup ensures that F1 team data is clean, standardized, and ready for analysis.

I am personally fascinated by Formula 1 because it combines cutting-edge technology, teamwork, and strategy in a highly competitive environment. Each team’s history, performance, and innovations offer rich insights, which makes the data particularly interesting to analyze and visualize.

Key steps of the pipeline:

Extract: Fetch raw team data from a reliable source.

Data Source: API-SPORTS F1 API
 provides official Formula 1 team data, including team names, locations, championship wins, and technical specifications. Alternatively, CSV files can be used for offline extraction.

Transform: Clean and standardize the data.

Fill missing values for fields like team location and chassis.

Standardize column names to lowercase with underscores.

Convert numeric columns to consistent data types.

Ensure text fields have consistent formatting (e.g., uppercase for team names).

Load: Store the cleaned data into a SQLite database for further analysis or reporting.

The database allows for fast queries, future expansions, and integration with visualization tools.

Analysis (Optional): Visualize key metrics such as:

Number of championships per team

Fastest laps or race performance

Geographical distribution of team bases
