{{
    config(
        materialized='table'
    )
}}

-- Report model optimized for Streamlit airline performance dashboard
-- Supports filtering by: airline, date, week, month, or all-time
-- Industry standard definitions for on-time, delayed, early, cancelled

with airline_punctuality_stats as (
    select
        -- Airline dimension (for filtering)
        airline_iata,
        airline_name,
        
        -- Time dimensions (for flexible filtering)
        flight_date,
        date_trunc('week', flight_date) as flight_week,
        date_trunc('month', flight_date) as flight_month,
        extract(year from flight_date) as flight_year,
        extract(week from flight_date) as week_number,
        extract(month from flight_date) as month_number,
        
        -- Flight type
        flight_type,
        
        -- Domestic vs International dimension (for filtering)
        is_domestic,
        
        -- Total flights (excluding deleted)
        count(*) filter (where not is_deleted) as total_flights,
        
        -- Punctuality categories (industry standard definitions)
        -- On-Time: < 15 minutes delay
        count(*) filter (
            where actual_time_utc is not null 
            and delay_minutes < 15 
            and delay_minutes >= 0
        ) as on_time_flights,
        
        -- Delayed: >= 15 minutes late
        count(*) filter (
            where actual_time_utc is not null 
            and delay_minutes >= 15
        ) as delayed_flights,
        
        -- Ahead of Schedule: negative delay (arrived/departed early)
        count(*) filter (
            where actual_time_utc is not null 
            and delay_minutes < 0
        ) as early_flights,
        
        -- Cancelled: status = CAN
        count(*) filter (where is_cancelled) as cancelled_flights,
        
        -- Completed flights (with actual times, excludes cancelled/deleted)
        count(*) filter (where actual_time_utc is not null) as completed_flights,
        
        -- Delay statistics (only for completed flights)
        round(avg(delay_minutes) filter (where actual_time_utc is not null), 2) as avg_delay_minutes,
        round(min(delay_minutes) filter (where actual_time_utc is not null), 2) as min_delay_minutes,
        round(max(delay_minutes) filter (where actual_time_utc is not null), 2) as max_delay_minutes,
        round(percentile_cont(0.5) within group (order by delay_minutes) filter (where actual_time_utc is not null), 2) as median_delay_minutes,
        
        -- Domestic vs International
        count(*) filter (where is_domestic and not is_deleted) as domestic_flights,
        count(*) filter (where not is_domestic and not is_deleted) as international_flights
        
    from {{ ref('int_flights') }}
    where airline_iata is not null
    group by 
        airline_iata,
        airline_name,
        flight_date,
        flight_week,
        flight_month,
        flight_year,
        week_number,
        month_number,
        flight_type,
        is_domestic
)

select
    {{ dbt_utils.generate_surrogate_key(['airline_iata', 'flight_date', 'flight_type', 'is_domestic']) }} as airline_punctuality_key,
    *,
    
    -- Calculate percentages (for Streamlit display)
    round(on_time_flights * 100.0 / nullif(completed_flights, 0), 2) as on_time_percentage,
    round(delayed_flights * 100.0 / nullif(completed_flights, 0), 2) as delayed_percentage,
    round(early_flights * 100.0 / nullif(completed_flights, 0), 2) as early_percentage,
    round(cancelled_flights * 100.0 / nullif(total_flights, 0), 2) as cancelled_percentage
    
from airline_punctuality_stats
order by airline_iata, flight_date, flight_type, is_domestic
