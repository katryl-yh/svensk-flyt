{{
    config(
        materialized='table'
    )
}}

-- Report model optimized for Streamlit punctuality dashboard
-- Supports filtering by: airport, date, week, month
-- Provides punctuality metrics: on-time, early, delayed, cancelled

with airport_punctuality_stats as (
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
        
        -- Flight type
        flight_type,
        
        -- Domestic vs International dimension (for filtering)
        is_domestic,
        
        -- Flight counts by punctuality status
        count(*) filter (where not is_deleted) as total_flights,
        count(*) filter (where is_cancelled) as cancelled_flights,
        count(*) filter (where actual_time_utc is not null) as completed_flights,
        
        -- Punctuality categories (only for completed flights with actual times)
        count(*) filter (where actual_time_utc is not null and delay_minutes < 0) as ahead_of_schedule_flights,
        count(*) filter (where is_on_time and actual_time_utc is not null) as on_time_flights,
        count(*) filter (where actual_time_utc is not null and delay_minutes > 15) as delayed_flights,
        
        -- Delay statistics (only for completed flights)
        round(avg(delay_minutes) filter (where actual_time_utc is not null), 2) as avg_delay_minutes,
        round(min(delay_minutes) filter (where actual_time_utc is not null), 2) as min_delay_minutes,
        round(max(delay_minutes) filter (where actual_time_utc is not null), 2) as max_delay_minutes,
        round(percentile_cont(0.5) within group (order by delay_minutes) filter (where actual_time_utc is not null), 2) as median_delay_minutes,
        
        -- Domestic vs International breakdown
        count(*) filter (where is_domestic and not is_deleted) as domestic_flights,
        count(*) filter (where not is_domestic and not is_deleted) as international_flights
        
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
        flight_type,
        is_domestic
),

with_percentages as (
    select
        *,
        -- Calculate percentages (based on total non-deleted flights)
        round(cancelled_flights * 100.0 / nullif(total_flights, 0), 2) as cancelled_percentage,
        round(ahead_of_schedule_flights * 100.0 / nullif(completed_flights, 0), 2) as ahead_of_schedule_percentage,
        round(on_time_flights * 100.0 / nullif(completed_flights, 0), 2) as on_time_percentage,
        round(delayed_flights * 100.0 / nullif(completed_flights, 0), 2) as delayed_percentage,
        round(completed_flights * 100.0 / nullif(total_flights, 0), 2) as completion_rate
    from airport_punctuality_stats
)

select
    {{ dbt_utils.generate_surrogate_key(['airport_iata', 'flight_date', 'flight_type', 'is_domestic']) }} as punctuality_key,
    *
from with_percentages
order by airport_iata, flight_date, flight_type, is_domestic
