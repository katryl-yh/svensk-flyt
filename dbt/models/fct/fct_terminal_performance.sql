{{
    config(
        materialized='table'
    )
}}

with terminal_stats as (
    select
        terminal,
        flight_type,
        flight_date,
        
        -- KPI: Terminal efficiency
        count(*) filter (where not is_deleted) as flight_count,
        count(distinct gate) as gates_used,
        count(*) filter (where is_domestic and not is_deleted) as domestic_flights,
        count(*) filter (where not is_domestic and not is_deleted) as international_flights,
        
        -- Punctuality by terminal
        count(*) filter (where is_on_time and actual_time_utc is not null) as on_time_flights,
        count(*) filter (where actual_time_utc is not null) as completed_flights,
        round(
            count(*) filter (where is_on_time and actual_time_utc is not null) * 100.0 / 
            nullif(count(*) filter (where actual_time_utc is not null), 0), 
            2
        ) as on_time_percentage,
        
        -- Delay metrics
        round(avg(delay_minutes) filter (where actual_time_utc is not null), 2) as avg_delay_minutes,
        
        -- Unique airlines
        count(distinct airline_iata) as unique_airlines
        
    from {{ ref('int_flights') }}
    where terminal is not null
    group by terminal, flight_type, flight_date
)

select
    {{ dbt_utils.generate_surrogate_key(['terminal', 'flight_type', 'flight_date']) }} as terminal_performance_key,
    *
from terminal_stats
order by terminal, flight_date, flight_type
