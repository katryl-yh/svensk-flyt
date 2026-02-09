-- Test: First bag time should be before or equal to last bag time
-- Baggage handling: first bag on carousel should arrive before (or at same time as) last bag
-- This catches data entry errors or API inconsistencies

select
    flight_key,
    flight_id,
    flight_number,
    airline_name,
    origin_airport_iata,
    destination_airport_iata,
    flight_date,
    actual_time_utc,
    first_bag_utc,
    last_bag_utc,
    
    -- Show how illogical the times are
    date_diff('minute', first_bag_utc, last_bag_utc) as baggage_handling_minutes,
    
    'First bag after last bag' as issue_type
    
from {{ ref('fct_flights') }}
where 
    first_bag_utc is not null
    and last_bag_utc is not null
    and first_bag_utc > last_bag_utc  -- Illogical: first bag came AFTER last bag
