{{
    config(
        materialized='table'
    )
}}

-- Mart model aggregating from atomic fct_flights
-- Provides route popularity metrics for Streamlit dashboard

with route_stats as (
    select
        -- Airport dimension (the airport being analyzed)
        case 
            when f.flight_type = 'arrival' then dest_ap.airport_iata
            when f.flight_type = 'departure' then orig_ap.airport_iata
        end as airport_iata,
        
        -- Route identification (directional)
        f.route_key,
        orig_ap.airport_iata as origin_airport_iata,
        dest_ap.airport_iata as destination_airport_iata,
        
        -- Other endpoint airport (the connected airport)
        case 
            when f.flight_type = 'arrival' then orig_ap.airport_iata
            when f.flight_type = 'departure' then dest_ap.airport_iata
        end as other_airport_iata,
        
        -- Flight direction
        f.flight_type,
        
        -- Domestic/International
        f.is_domestic,
        
        -- Join to dim_date for time attributes
        d.date_day as flight_date,
        d.week_start_date as flight_week,
        date_trunc('month', d.date_day) as flight_month,
        d.year as flight_year,
        d.week_number,
        d.month as month_number,
        
        -- Metrics
        count(*) filter (where not f.is_deleted) as flight_count,
        count(distinct f.airline_key) as unique_airlines,
        
        -- Cancelled flights on this route
        count(*) filter (where f.is_cancelled) as cancelled_flights
        
    from {{ ref('fct_flights') }} f
    inner join {{ ref('dim_date') }} d on f.flight_date_key = d.date_key
    inner join {{ ref('dim_airport') }} orig_ap on f.origin_airport_key = orig_ap.airport_key
    inner join {{ ref('dim_airport') }} dest_ap on f.dest_airport_key = dest_ap.airport_key
    where (
        -- Filter to Swedish airports only
        (f.flight_type = 'arrival' and dest_ap.airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN'))
        or
        (f.flight_type = 'departure' and orig_ap.airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN'))
    )
    group by 
        1,  -- airport_iata (CASE expression)
        f.route_key,
        orig_ap.airport_iata,
        dest_ap.airport_iata,
        2,  -- other_airport_iata (CASE expression)
        f.flight_type,
        f.is_domestic,
        d.date_day,
        d.week_start_date,
        date_trunc('month', d.date_day),
        d.year,
        d.week_number,
        d.month
)

select
    {{ dbt_utils.generate_surrogate_key(['airport_iata', 'route_key', 'flight_date', 'flight_type']) }} as route_popularity_key,
    *
from route_stats
order by airport_iata, flight_date, flight_count desc
