{{ config(materialized='view') }}

with source as (
    
    SELECT * FROM {{ source('f1_raw', 'teams') }}
),

renamed_and_cast as (
    SELECT 
        -- IDs should be cast to INT64 for proper joining
        CAST(id AS INT64) as team_id,
        
        -- Strings: Standardize casing and handle NULLs
        name as team_name,
        logo as team_logo_url,
        COALESCE(base, 'Unknown') as home_base,
        director as director_name,
        COALESCE(technical_manager, 'Unknown') as tech_manager,
        COALESCE(chassis, 'Unknown') as chassis_code,
        engine as engine_manufacturer,
        tyres as tyre_supplier,

        -- Numeric: CAST to INT64 so you can SUM/AVG later
        CAST(COALESCE(first_team_entry, 0) AS INT64) as debut_year,
        CAST(COALESCE(world_championships, 0) AS INT64) as world_championships_count,
        CAST(COALESCE(pole_positions, 0) AS INT64) as total_pole_positions,
        CAST(COALESCE(fastest_laps, 0) AS INT64) as total_fastest_laps,

        -- Records: Keep as is for unnesting in the Intermediate layer
        highest_race_finish as highest_finish_record

    FROM source
)

SELECT * FROM renamed_and_cast