{{
    config(
        materialized='table'
    )
}}

-- Report model optimized for Streamlit baggage performance dashboard
-- Supports filtering by: airport, baggage claim unit, time of day, day of week, date, week, month
-- Grain: airport + date + domestic/international + baggage_claim_unit + time_period + day_of_week

with baggage_stats as (
    select
        -- Airport dimension (arrivals only have baggage data)
        destination_airport_iata as airport_iata,
        
        -- Baggage infrastructure
        baggage_claim_unit,
        
        -- Domestic/International
        is_domestic,
        
        -- Time dimensions (for flexible filtering)
        flight_date,
        date_trunc('week', flight_date) as flight_week,
        date_trunc('month', flight_date) as flight_month,
        extract(year from flight_date) as flight_year,
        extract(week from flight_date) as week_number,
        extract(month from flight_date) as month_number,
        
        -- Time of day patterns
        flight_hour,
        flight_time_period,  -- Morning/Midday/Evening/Night
        
        -- Day of week patterns
        flight_day_of_week,  -- 1-7 (Mon-Sun)
        flight_day_name,     -- 'Monday', 'Tuesday', etc.
        
        -- Baggage performance metrics
        count(*) filter (where baggage_handling_minutes is not null) as flights_with_baggage_data,
        count(*) as total_arrivals,
        
        -- Central tendency
        round(avg(baggage_handling_minutes), 2) as avg_baggage_handling_minutes,
        round(percentile_cont(0.5) within group (order by baggage_handling_minutes), 2) as median_baggage_handling_minutes,
        
        -- Distribution metrics
        round(min(baggage_handling_minutes), 2) as min_baggage_handling_minutes,
        round(max(baggage_handling_minutes), 2) as max_baggage_handling_minutes,
        round(percentile_cont(0.90) within group (order by baggage_handling_minutes), 2) as p90_baggage_handling_minutes,
        round(percentile_cont(0.95) within group (order by baggage_handling_minutes), 2) as p95_baggage_handling_minutes,
        
        -- Correlation with flight delays
        round(avg(delay_minutes) filter (where baggage_handling_minutes is not null), 2) as avg_flight_delay_minutes,
        round(percentile_cont(0.5) within group (order by delay_minutes) filter (where baggage_handling_minutes is not null), 2) as median_flight_delay_minutes
        
    from {{ ref('int_flights') }}
    where flight_type = 'arrival'  -- Only arrivals have baggage data
      and destination_airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN')
      and baggage_claim_unit is not null
    group by 
        airport_iata,
        baggage_claim_unit,
        is_domestic,
        flight_date,
        flight_week,
        flight_month,
        flight_year,
        week_number,
        month_number,
        flight_hour,
        flight_time_period,
        flight_day_of_week,
        flight_day_name
)

select
    {{ dbt_utils.generate_surrogate_key(['airport_iata', 'baggage_claim_unit', 'flight_date', 'is_domestic', 'flight_hour', 'flight_day_of_week']) }} as baggage_performance_key,
    *
from baggage_stats
order by airport_iata, flight_date, baggage_claim_unit
