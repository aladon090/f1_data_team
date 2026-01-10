{{ config(materialized='table') }}

with team_base as (
    select * from {{ ref('int_f1_team_success') }}
)

select
    team_name,
    world_championships_count,
    current_years_active,
    era_category,
    -- Analytical Logic: Is the team "Peaking" or "Legacy"?
    case 
        when success_rating > 0.15 then 'Dominant Dynasty'
        when success_rating > 0.05 then 'Competitive'
        when world_championships_count > 0 then 'Past Glory'
        else 'Developing'
    end as momentum_status,
    -- Flag for teams that won titles recently (relative to their age)
    (world_championships_count > 0 and era_category = 'Contemporary Era') as is_modern_winner
from team_base