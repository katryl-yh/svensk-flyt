-- Test: Flight date should match the date portion of scheduled_time_utc
-- The flight_date column should align with the actual scheduled timestamp
-- Mismatches indicate data extraction or transformation errors

select
    flight_key,
    flight_id,
    flight_number,
    airline_name,
    origin_airport_iata,
    destination_airport_iata,
    flight_type,
    flight_date,
    scheduled_time_utc,
    
    -- Extract date from scheduled timestamp
    cast(scheduled_time_utc as date) as scheduled_date,
    
    -- Show the mismatch
    date_diff('day', flight_date, cast(scheduled_time_utc as date)) as day_difference,
    
    'Flight date does not match scheduled date' as issue_type
    
from {{ ref('fct_flights') }}
where 
    scheduled_time_utc is not null
    and flight_date != cast(scheduled_time_utc as date)
