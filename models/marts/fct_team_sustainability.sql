{{ config(materialized='table') }}

select
    team_name,
    case 
        when success_rating > 0.1 then 'High Performer'
        when success_rating > 0 and success_rating <= 0.1 then 'Consistent'
        else 'Underperformer'
    end as performance_tier,
    case 
        when current_years_active > 40 then 'Stable/Legacy'
        when current_years_active between 10 and 40 then 'Established'
        else 'High Risk/Newcomer'
    end as stability_profile
from {{ ref('int_f1_team_success') }}