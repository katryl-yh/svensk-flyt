{{
    config(
        materialized='table'
    )
}}

-- Mart model aggregating from atomic fct_flights
-- Provides airport punctuality metrics for Streamlit dashboard

with airport_punctuality_stats as (
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
        
        -- Flight type
        f.flight_type,
        
        -- Domestic vs International dimension (for filtering)
        f.is_domestic,
        
        -- Flight counts by punctuality status
        count(*) filter (where not f.is_deleted) as total_flights,
        count(*) filter (where f.is_cancelled) as cancelled_flights,
        count(*) filter (where f.actual_time_utc is not null) as completed_flights,
        
        -- Punctuality categories (only for completed flights with actual times)
        count(*) filter (where f.actual_time_utc is not null and f.delay_minutes < 0) as ahead_of_schedule_flights,
        count(*) filter (where f.is_on_time and f.actual_time_utc is not null) as on_time_flights,
        count(*) filter (where f.actual_time_utc is not null and f.delay_minutes > 15) as delayed_flights,
        
        -- Delay statistics (only for completed flights)
        round(avg(f.delay_minutes) filter (where f.actual_time_utc is not null), 2) as avg_delay_minutes,
        round(min(f.delay_minutes) filter (where f.actual_time_utc is not null), 2) as min_delay_minutes,
        round(max(f.delay_minutes) filter (where f.actual_time_utc is not null), 2) as max_delay_minutes,
        round(percentile_cont(0.5) within group (order by f.delay_minutes) filter (where f.actual_time_utc is not null), 2) as median_delay_minutes,
        
        -- Domestic vs International breakdown
        count(*) filter (where f.is_domestic and not f.is_deleted) as domestic_flights,
        count(*) filter (where not f.is_domestic and not f.is_deleted) as international_flights
        
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
        f.flight_type,
        f.is_domestic
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
