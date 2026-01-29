{{
    config(
        materialized='table'
    )
}}

with airport_daily_stats as (
    select
        case 
            when flight_type = 'arrival' then destination_airport_iata
            when flight_type = 'departure' then origin_airport_iata
        end as airport_iata,
        flight_date,
        flight_type,
        
        -- KPI: Capacity utilization
        count(*) filter (where not is_deleted) as flight_count,
        count(*) filter (where is_domestic and not is_deleted) as domestic_flights,
        count(*) filter (where not is_domestic and not is_deleted) as international_flights,
        
        -- Operational metrics
        count(*) filter (where is_cancelled) as cancelled_flights,
        count(*) filter (where is_deleted) as deleted_flights,
        
        -- Punctuality
        count(*) filter (where is_on_time and actual_time_utc is not null) as on_time_flights,
        count(*) filter (where actual_time_utc is not null) as completed_flights,
        round(
            count(*) filter (where is_on_time and actual_time_utc is not null) * 100.0 / 
            nullif(count(*) filter (where actual_time_utc is not null), 0), 
            2
        ) as on_time_percentage,
        
        -- Unique airlines operating
        count(distinct airline_iata) as unique_airlines
        
    from {{ ref('int_flights') }}
    where (
        (flight_type = 'arrival' and destination_airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN'))
        or
        (flight_type = 'departure' and origin_airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN'))
    )
    group by airport_iata, flight_date, flight_type
)

select
    {{ dbt_utils.generate_surrogate_key(['airport_iata', 'flight_date', 'flight_type']) }} as airport_daily_key,
    *
from airport_daily_stats
order by airport_iata, flight_date, flight_type
