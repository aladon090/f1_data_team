{{ config(materialized='table') }}

with team_stats as (
    select * from {{ ref('int_f1_team_success') }}
)

select
    engine_manufacturer,
    count(team_id) as number_of_teams,
    sum(world_championships_count) as total_titles_by_engine,
    round(avg(success_rating), 3) as avg_engine_efficiency,
    -- Identifies which engine is the most "Modern"
    round(avg(current_years_active), 1) as avg_team_longevity
from team_stats
group by 1
order by avg_engine_efficiency desc