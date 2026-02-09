-- Test: Delay minutes should be within reasonable bounds
-- Delays beyond -180 minutes (3 hours early) or +720 minutes (12 hours late) are likely data errors
-- Industry context: Most delays are under 6 hours; extremely early arrivals are rare

select
    flight_key,
    flight_id,
    flight_number,
    airline_name,
    origin_airport_iata,
    destination_airport_iata,
    flight_date,
    scheduled_time_utc,
    actual_time_utc,
    delay_minutes,
    
    case 
        when delay_minutes < -180 then 'Extremely early (>3 hours)'
        when delay_minutes > 720 then 'Extremely delayed (>12 hours)'
    end as issue_type
    
from {{ ref('fct_flights') }}
where 
    actual_time_utc is not null  -- Only check completed flights
    and (
        delay_minutes < -180  -- More than 3 hours early
        or delay_minutes > 720  -- More than 12 hours late
    )
