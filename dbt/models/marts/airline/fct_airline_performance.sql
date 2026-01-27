{{
    config(
        materialized='table'
    )
}}

with airline_stats as (
    select
        airline_iata,
        airline_name,
        
        -- Overall counts
        count(*) as total_flights,
        count(*) filter (where not is_deleted) as active_flights,
        count(*) filter (where is_cancelled) as cancelled_flights,
        count(*) filter (where is_deleted) as deleted_flights,
        
        -- Landed/departed flights with actual times
        count(*) filter (where actual_time_utc is not null) as completed_flights,
        
        -- Punctuality metrics (KPI: Punctuality)
        count(*) filter (where is_on_time and actual_time_utc is not null) as on_time_flights,
        round(
            count(*) filter (where is_on_time and actual_time_utc is not null) * 100.0 / 
            nullif(count(*) filter (where actual_time_utc is not null), 0), 
            2
        ) as on_time_percentage,
        
        -- Delay statistics (KPI: Airline performance)
        round(avg(delay_minutes) filter (where actual_time_utc is not null), 2) as avg_delay_minutes,
        round(min(delay_minutes) filter (where actual_time_utc is not null), 2) as best_early_minutes,
        round(max(delay_minutes) filter (where actual_time_utc is not null), 2) as worst_late_minutes,
        round(percentile_cont(0.5) within group (order by delay_minutes) filter (where actual_time_utc is not null), 2) as median_delay_minutes,
        
        -- Domestic vs International
        count(*) filter (where is_domestic and not is_deleted) as domestic_flights,
        count(*) filter (where not is_domestic and not is_deleted) as international_flights
        
    from {{ ref('int_flights') }}
    where airline_iata is not null
    group by airline_iata, airline_name
)

select
    {{ dbt_utils.generate_surrogate_key(['airline_iata']) }} as airline_key,
    *
from airline_stats
order by active_flights desc
