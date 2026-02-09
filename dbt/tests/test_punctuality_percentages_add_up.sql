-- Test: Punctuality percentages should sum to approximately 100%
-- In mart_airline_punctuality, on_time + delayed should equal 100%
-- Note: early_percentage is a SUBSET of on_time_percentage (for informational purposes)
-- Small rounding differences (<0.1%) are acceptable due to floating point math

with percentage_validation as (
    select
        airline_punctuality_key,
        airline_iata,
        flight_date,
        flight_type,
        completed_flights,
        on_time_percentage,
        delayed_percentage,
        early_percentage,
        
        -- Sum of main categories only (early is subset of on-time)
        (on_time_percentage + delayed_percentage) as total_percentage,
        
        -- Calculate expected total (should be 100% for completed flights)
        case 
            when completed_flights > 0 then 100.0
            else 0.0
        end as expected_percentage
        
    from {{ ref('mart_airline_punctuality') }}
    where completed_flights > 0  -- Only check rows with actual completed flights
)

-- Return rows where percentages don't add up (allowing 0.1% tolerance for rounding)
select
    airline_iata,
    flight_date,
    flight_type,
    completed_flights,
    on_time_percentage,
    delayed_percentage,
    early_percentage,
    total_percentage,
    expected_percentage,
    abs(total_percentage - expected_percentage) as difference
from percentage_validation
where abs(total_percentage - expected_percentage) > 0.1  -- Fail if difference > 0.1%
