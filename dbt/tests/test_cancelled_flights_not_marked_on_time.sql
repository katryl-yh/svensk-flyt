-- Test: Cancelled flights should not be marked as on-time
-- If a flight is cancelled, it cannot have on-time status
-- This catches business logic errors in the is_on_time flag

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
    is_cancelled,
    is_on_time,
    delay_minutes,
    
    'Cancelled flight marked as on-time' as issue_type
    
from {{ ref('fct_flights') }}
where 
    is_cancelled = true
    and is_on_time = true  -- This combination doesn't make sense
