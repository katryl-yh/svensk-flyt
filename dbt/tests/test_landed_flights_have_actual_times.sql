-- Test: Flights marked as 'landed' should have actual arrival times
-- If flight status is 'LAN' (landed), the actual_time_utc should not be null
-- This catches incomplete API responses or transformation errors

select
    flight_key,
    flight_id,
    flight_number,
    airline_name,
    origin_airport_iata,
    destination_airport_iata,
    flight_type,
    flight_date,
    flight_status,
    scheduled_time_utc,
    actual_time_utc,
    is_landed,
    
    'Landed flight missing actual time' as issue_type
    
from {{ ref('fct_flights') }}
where 
    is_landed = true
    and actual_time_utc is null
