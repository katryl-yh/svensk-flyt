-- Test: Actual flight times should not be far in the future
-- Scheduled times can be in future, but actual times should be historical (or very recent)
-- Allow 1 hour buffer for timezone issues and data sync delays

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
    actual_time_utc,
    
    -- Show how far in the future
    date_diff('hour', current_timestamp, actual_time_utc) as hours_in_future,
    
    'Actual time is in the future' as issue_type
    
from {{ ref('fct_flights') }}
where 
    actual_time_utc is not null
    and actual_time_utc > current_timestamp + INTERVAL 1 HOUR  -- More than 1 hour in future
