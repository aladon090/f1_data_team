{{ config(materialized='view') }}

with teams as (
    select * from {{ ref('stg_f1_teams') }}
)
, inter AS (
    SELECT
        *,
        
        -- [TASK 1: LONGEVITY]
        CASE 
            WHEN debut_year != 0 THEN (EXTRACT(YEAR FROM current_date()) - debut_year) 
            ELSE 0 
        END AS current_years_active,
        
        -- [TASK 2: SUCCESS RATING]
        CASE 
            WHEN world_championships_count != 0 
            THEN ROUND(CAST(world_championships_count AS FLOAT64) / (2026 - debut_year),2 )
            ELSE 0 
        END AS success_rating,
        
        -- [TASK 3: ERA CATEGORIZATION]
        CASE
            WHEN debut_year = 0 THEN 'NO DATA'
            WHEN debut_year <= 1950 THEN 'Early Era'
            WHEN debut_year BETWEEN 1955 AND 1999 THEN 'Modern Era'
            WHEN debut_year >= 2000 THEN 'Contemporary Era'
            ELSE 'Unknown Era'
        END AS era_category

    FROM teams
    ORDER BY debut_year DESC
)
SELECT * FROM inter

