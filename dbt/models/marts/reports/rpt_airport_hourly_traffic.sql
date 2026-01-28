{{
    config(
        materialized='table'
    )
}}

-- Report model optimized for Streamlit peak hours dashboard
-- Supports filtering by: airport, date, week, month, or all-time
-- Provides hourly traffic patterns per airport

with airport_hourly_stats as (
    select
        -- Airport dimension (for filtering)
        case 
            when flight_type = 'arrival' then destination_airport_iata
            when flight_type = 'departure' then origin_airport_iata
        end as airport_iata,
        
        -- Time dimensions (for flexible filtering)
        flight_date,
        date_trunc('week', flight_date) as flight_week,
        date_trunc('month', flight_date) as flight_month,
        extract(year from flight_date) as flight_year,
        extract(week from flight_date) as week_number,
        extract(month from flight_date) as month_number,
        
        -- Hour dimensions (for peak analysis)
        flight_hour,
        flight_time_period,
        flight_type,
        
        -- Metrics
        count(*) filter (where not is_deleted) as flight_count,
        count(*) filter (where is_domestic and not is_deleted) as domestic_flights,
        count(*) filter (where not is_domestic and not is_deleted) as international_flights,
        count(distinct airline_iata) as unique_airlines,
        
        -- Delay metrics
        round(avg(delay_minutes) filter (where actual_time_utc is not null), 2) as avg_delay_minutes,
        count(*) filter (where is_on_time and actual_time_utc is not null) as on_time_flights,
        count(*) filter (where actual_time_utc is not null) as completed_flights
        
    from {{ ref('int_flights') }}
    where (
        -- Filter to Swedish airports only
        (flight_type = 'arrival' and destination_airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN'))
        or
        (flight_type = 'departure' and origin_airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN'))
    )
    group by 
        airport_iata,
        flight_date,
        flight_week,
        flight_month,
        flight_year,
        week_number,
        month_number,
        flight_hour,
        flight_time_period,
        flight_type
)

select
    {{ dbt_utils.generate_surrogate_key(['airport_iata', 'flight_date', 'flight_hour', 'flight_type']) }} as hourly_traffic_key,
    *
from airport_hourly_stats
order by airport_iata, flight_date, flight_hour, flight_type
