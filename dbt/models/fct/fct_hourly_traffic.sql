{{
    config(
        materialized='table'
    )
}}

with hourly_stats as (
    select
        flight_hour,
        flight_time_period,
        flight_type,
        
        -- KPI: Peak hours
        count(*) filter (where not is_deleted) as flight_count,
        count(distinct airline_iata) as unique_airlines,
        
        -- Domestic vs International breakdown
        count(*) filter (where is_domestic and not is_deleted) as domestic_flights,
        count(*) filter (where not is_domestic and not is_deleted) as international_flights,
        
        -- Delay metrics
        round(avg(delay_minutes) filter (where actual_time_utc is not null), 2) as avg_delay_minutes,
        count(*) filter (where is_on_time and actual_time_utc is not null) as on_time_flights,
        count(*) filter (where actual_time_utc is not null) as completed_flights
        
    from {{ ref('int_flights') }}
    where flight_hour is not null
    group by flight_hour, flight_time_period, flight_type
)

select
    {{ dbt_utils.generate_surrogate_key(['flight_hour', 'flight_time_period', 'flight_type']) }} as hourly_traffic_key,
    *
from hourly_stats
order by flight_type, flight_hour
