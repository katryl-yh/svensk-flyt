{{
    config(
        materialized='table'
    )
}}

-- Report model optimized for Streamlit route popularity dashboard
-- Supports filtering by: airport, direction (arrival/departure), date, week, month
-- Routes are directional: ARN→GOT is separate from GOT→ARN

with route_stats as (
    select
        -- Airport dimension (the airport being analyzed)
        case 
            when flight_type = 'arrival' then destination_airport_iata
            when flight_type = 'departure' then origin_airport_iata
        end as airport_iata,
        
        -- Route identification (directional)
        route_key,  -- e.g., 'CPH-ARN' or 'ARN-GOT'
        origin_airport_iata,
        destination_airport_iata,
        
        -- Other endpoint airport (the connected airport)
        case 
            when flight_type = 'arrival' then origin_airport_iata
            when flight_type = 'departure' then destination_airport_iata
        end as other_airport_iata,
        
        -- Flight direction
        flight_type,
        
        -- Domestic/International
        is_domestic,
        
        -- Time dimensions (for flexible filtering)
        flight_date,
        date_trunc('week', flight_date) as flight_week,
        date_trunc('month', flight_date) as flight_month,
        extract(year from flight_date) as flight_year,
        extract(week from flight_date) as week_number,
        extract(month from flight_date) as month_number,
        
        -- Metrics
        count(*) filter (where not is_deleted) as flight_count,
        count(distinct airline_iata) as unique_airlines,
        
        -- Cancelled flights on this route
        count(*) filter (where is_cancelled) as cancelled_flights
        
    from {{ ref('int_flights') }}
    where (
        -- Filter to Swedish airports only
        (flight_type = 'arrival' and destination_airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN'))
        or
        (flight_type = 'departure' and origin_airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN'))
    )
    group by 
        airport_iata,
        route_key,
        origin_airport_iata,
        destination_airport_iata,
        other_airport_iata,
        flight_type,
        is_domestic,
        flight_date,
        flight_week,
        flight_month,
        flight_year,
        week_number,
        month_number
)

select
    {{ dbt_utils.generate_surrogate_key(['airport_iata', 'route_key', 'flight_date', 'flight_type']) }} as route_popularity_key,
    *
from route_stats
order by airport_iata, flight_date, flight_count desc
