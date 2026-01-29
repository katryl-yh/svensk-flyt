{{
    config(
        materialized='table'
    )
}}

-- Mart model aggregating from atomic fct_flights
-- Provides airline performance metrics for Streamlit dashboard

with airline_punctuality_stats as (
    select
        -- Join to get airline name
        a.airline_iata,
        a.airline_name,
        
        -- Join to dim_date for time attributes
        d.date_day as flight_date,
        d.week_start_date as flight_week,
        date_trunc('month', d.date_day) as flight_month,
        d.year as flight_year,
        d.week_number,
        d.month as month_number,
        
        -- Flight type
        f.flight_type,
        
        -- Domestic vs International dimension (for filtering)
        f.is_domestic,
        
        -- Total flights (excluding deleted)
        count(*) filter (where not f.is_deleted) as total_flights,
        
        -- Punctuality categories (industry standard definitions)
        -- On-Time: < 15 minutes delay
        count(*) filter (
            where f.actual_time_utc is not null 
            and f.delay_minutes < 15 
            and f.delay_minutes >= 0
        ) as on_time_flights,
        
        -- Delayed: >= 15 minutes late
        count(*) filter (
            where f.actual_time_utc is not null 
            and f.delay_minutes >= 15
        ) as delayed_flights,
        
        -- Ahead of Schedule: negative delay (arrived/departed early)
        count(*) filter (
            where f.actual_time_utc is not null 
            and f.delay_minutes < 0
        ) as early_flights,
        
        -- Cancelled: status = CAN
        count(*) filter (where f.is_cancelled) as cancelled_flights,
        
        -- Completed flights (with actual times, excludes cancelled/deleted)
        count(*) filter (where f.actual_time_utc is not null) as completed_flights,
        
        -- Delay statistics (only for completed flights)
        round(avg(f.delay_minutes) filter (where f.actual_time_utc is not null), 2) as avg_delay_minutes,
        round(min(f.delay_minutes) filter (where f.actual_time_utc is not null), 2) as min_delay_minutes,
        round(max(f.delay_minutes) filter (where f.actual_time_utc is not null), 2) as max_delay_minutes,
        round(percentile_cont(0.5) within group (order by f.delay_minutes) filter (where f.actual_time_utc is not null), 2) as median_delay_minutes,
        
        -- Domestic vs International
        count(*) filter (where f.is_domestic and not f.is_deleted) as domestic_flights,
        count(*) filter (where not f.is_domestic and not f.is_deleted) as international_flights
        
    from {{ ref('fct_flights') }} f
    inner join {{ ref('dim_airline') }} a on f.airline_key = a.airline_key
    inner join {{ ref('dim_date') }} d on f.flight_date_key = d.date_key
    group by 
        a.airline_iata,
        a.airline_name,
        d.date_day,
        d.week_start_date,
        date_trunc('month', d.date_day),
        d.year,
        d.week_number,
        d.month,
        f.flight_type,
        f.is_domestic
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
