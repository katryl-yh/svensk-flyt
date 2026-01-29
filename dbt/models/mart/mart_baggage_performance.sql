{{
    config(
        materialized='table'
    )
}}

-- Mart model aggregating from atomic fct_flights
-- Provides baggage handling performance metrics for Streamlit dashboard

with baggage_stats as (
    select
        -- Airport dimension (arrivals only have baggage data)
        dest_ap.airport_iata,
        
        -- Baggage infrastructure
        f.baggage_claim_unit,
        
        -- Domestic/International
        f.is_domestic,
        
        -- Join to dim_date for time attributes
        d.date_day as flight_date,
        d.week_start_date as flight_week,
        date_trunc('month', d.date_day) as flight_month,
        d.year as flight_year,
        d.week_number,
        d.month as month_number,
        
        -- Time of day patterns
        f.flight_hour,
        f.flight_time_period,
        
        -- Day of week patterns
        f.flight_day_of_week,
        f.flight_day_name,
        
        -- Baggage performance metrics
        count(*) filter (where f.baggage_handling_minutes is not null) as flights_with_baggage_data,
        count(*) as total_arrivals,
        
        -- Central tendency
        round(avg(f.baggage_handling_minutes), 2) as avg_baggage_handling_minutes,
        round(percentile_cont(0.5) within group (order by f.baggage_handling_minutes), 2) as median_baggage_handling_minutes,
        
        -- Distribution metrics
        round(min(f.baggage_handling_minutes), 2) as min_baggage_handling_minutes,
        round(max(f.baggage_handling_minutes), 2) as max_baggage_handling_minutes,
        round(percentile_cont(0.90) within group (order by f.baggage_handling_minutes), 2) as p90_baggage_handling_minutes,
        round(percentile_cont(0.95) within group (order by f.baggage_handling_minutes), 2) as p95_baggage_handling_minutes,
        
        -- Correlation with flight delays
        round(avg(f.delay_minutes) filter (where f.baggage_handling_minutes is not null), 2) as avg_flight_delay_minutes,
        round(percentile_cont(0.5) within group (order by f.delay_minutes) filter (where f.baggage_handling_minutes is not null), 2) as median_flight_delay_minutes
        
    from {{ ref('fct_flights') }} f
    inner join {{ ref('dim_date') }} d on f.flight_date_key = d.date_key
    inner join {{ ref('dim_airport') }} dest_ap on f.dest_airport_key = dest_ap.airport_key
    where f.flight_type = 'arrival'  -- Only arrivals have baggage data
      and dest_ap.airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN')
      and f.baggage_claim_unit is not null
    group by 
        dest_ap.airport_iata,
        f.baggage_claim_unit,
        f.is_domestic,
        d.date_day,
        d.week_start_date,
        date_trunc('month', d.date_day),
        d.year,
        d.week_number,
        d.month,
        f.flight_hour,
        f.flight_time_period,
        f.flight_day_of_week,
        f.flight_day_name
)

select
    {{ dbt_utils.generate_surrogate_key(['airport_iata', 'baggage_claim_unit', 'flight_date', 'is_domestic', 'flight_hour', 'flight_day_of_week']) }} as baggage_performance_key,
    *
from baggage_stats
order by airport_iata, flight_date, baggage_claim_unit
