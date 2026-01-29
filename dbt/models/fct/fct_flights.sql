{{
    config(
        materialized='table'
    )
}}

-- Atomic grain fact table: one row per flight
-- Follows Kimball methodology with surrogate keys and FKs to dimensions

with flights_with_keys as (
    select
        -- Generate surrogate key for fact table
        -- Include scheduled_time_utc to handle same flight_id at different times
        {{ dbt_utils.generate_surrogate_key(['flight_id', 'flight_type', 'scheduled_time_utc']) }} as flight_key,
        
        -- Foreign keys to dimensions
        {{ dbt_utils.generate_surrogate_key(['airline_iata']) }} as airline_key,
        {{ dbt_utils.generate_surrogate_key(['origin_airport_iata']) }} as origin_airport_key,
        {{ dbt_utils.generate_surrogate_key(['destination_airport_iata']) }} as dest_airport_key,
        cast(strftime(flight_date, '%Y%m%d') as integer) as flight_date_key,
        
        -- Degenerate dimensions (attributes that don't warrant their own dimension)
        flight_id,
        flight_number,
        flight_type,
        flight_status,
        terminal,
        gate,
        route_key,
        
        -- Time attributes (kept for convenience, could also join to dim_date)
        scheduled_time_utc,
        actual_time_utc,
        flight_date,
        flight_hour,
        flight_day_of_week,
        flight_day_name,
        flight_time_period,
        
        -- Measures (numeric facts)
        delay_minutes,
        baggage_handling_minutes,
        
        -- Boolean measures
        is_deleted,
        is_cancelled,
        is_domestic,
        is_on_time,
        is_landed,
        
        -- Arrival-specific measures
        baggage_claim_unit,
        first_bag_utc,
        last_bag_utc,
        
        -- Metadata
        departure_date_utc,
        airline_name,  -- denormalized for convenience, but FK to dim_airline is primary
        origin_airport_iata,
        destination_airport_iata
        
    from {{ ref('int_flights') }}
)

select * from flights_with_keys
