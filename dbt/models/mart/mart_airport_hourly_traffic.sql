{{
    config(
        materialized='table'
    )
}}

-- Mart model aggregating from atomic fct_flights
-- Provides hourly traffic patterns per airport for Streamlit dashboard

with airport_hourly_stats as (
    select
        -- Join to get airport info
        case 
            when f.flight_type = 'arrival' then dest_ap.airport_iata
            when f.flight_type = 'departure' then orig_ap.airport_iata
        end as airport_iata,
        
        -- Join to dim_date for time attributes
        d.date_day as flight_date,
        d.week_start_date as flight_week,
        date_trunc('month', d.date_day) as flight_month,
        d.year as flight_year,
        d.week_number,
        d.month as month_number,
        
        -- Hour dimensions (for peak analysis)
        f.flight_hour,
        f.flight_time_period,
        f.flight_type,
        
        -- Metrics
        count(*) filter (where not f.is_deleted) as flight_count,
        count(*) filter (where f.is_domestic and not f.is_deleted) as domestic_flights,
        count(*) filter (where not f.is_domestic and not f.is_deleted) as international_flights,
        count(distinct f.airline_key) as unique_airlines,
        
        -- Delay metrics
        round(avg(f.delay_minutes) filter (where f.actual_time_utc is not null), 2) as avg_delay_minutes,
        count(*) filter (where f.is_on_time and f.actual_time_utc is not null) as on_time_flights,
        count(*) filter (where f.actual_time_utc is not null) as completed_flights
        
    from {{ ref('fct_flights') }} f
    inner join {{ ref('dim_date') }} d on f.flight_date_key = d.date_key
    left join {{ ref('dim_airport') }} orig_ap on f.origin_airport_key = orig_ap.airport_key
    left join {{ ref('dim_airport') }} dest_ap on f.dest_airport_key = dest_ap.airport_key
    where (
        -- Filter to Swedish airports only
        (f.flight_type = 'arrival' and dest_ap.airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN'))
        or
        (f.flight_type = 'departure' and orig_ap.airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN'))
    )
    group by 
        1,  -- airport_iata (CASE expression)
        d.date_day,
        d.week_start_date,
        date_trunc('month', d.date_day),
        d.year,
        d.week_number,
        d.month,
        f.flight_hour,
        f.flight_time_period,
        f.flight_type
)

select
    {{ dbt_utils.generate_surrogate_key(['airport_iata', 'flight_date', 'flight_hour', 'flight_type']) }} as hourly_traffic_key,
    *
from airport_hourly_stats
order by airport_iata, flight_date, flight_hour, flight_type
