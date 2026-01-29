{{
    config(
        materialized='table'
    )
}}

with gate_stats as (
    select
        terminal,
        gate,
        flight_type,
        flight_date,
        
        -- KPI: Gate management
        count(*) filter (where not is_deleted) as flight_count,
        count(distinct airline_iata) as unique_airlines,
        
        -- Domestic vs International
        count(*) filter (where is_domestic and not is_deleted) as domestic_flights,
        count(*) filter (where not is_domestic and not is_deleted) as international_flights,
        
        -- Punctuality
        count(*) filter (where is_on_time and actual_time_utc is not null) as on_time_flights,
        count(*) filter (where actual_time_utc is not null) as completed_flights,
        round(
            count(*) filter (where is_on_time and actual_time_utc is not null) * 100.0 / 
            nullif(count(*) filter (where actual_time_utc is not null), 0), 
            2
        ) as on_time_percentage,
        
        -- Delay metrics
        round(avg(delay_minutes) filter (where actual_time_utc is not null), 2) as avg_delay_minutes
        
    from {{ ref('int_flights') }}
    where gate is not null
    group by terminal, gate, flight_type, flight_date
)

select
    {{ dbt_utils.generate_surrogate_key(['terminal', 'gate', 'flight_type', 'flight_date']) }} as gate_utilization_key,
    *
from gate_stats
order by terminal, gate, flight_date, flight_type
