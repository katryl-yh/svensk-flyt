{{
    config(
        materialized='table'
    )
}}

with baggage_stats as (
    select
        baggage_claim_unit,
        airline_iata,
        airline_name,
        flight_date,
        
        -- KPI: Baggage handling efficiency
        count(*) filter (where baggage_handling_minutes is not null) as flights_with_baggage_data,
        round(avg(baggage_handling_minutes), 2) as avg_baggage_handling_minutes,
        round(min(baggage_handling_minutes), 2) as min_baggage_handling_minutes,
        round(max(baggage_handling_minutes), 2) as max_baggage_handling_minutes,
        round(percentile_cont(0.5) within group (order by baggage_handling_minutes), 2) as median_baggage_handling_minutes,
        
        -- Domestic vs International
        count(*) filter (where is_domestic and baggage_handling_minutes is not null) as domestic_flights,
        count(*) filter (where not is_domestic and baggage_handling_minutes is not null) as international_flights,
        
        -- Flight punctuality correlation
        round(avg(delay_minutes) filter (where baggage_handling_minutes is not null), 2) as avg_flight_delay_minutes,
        
        -- Total flights at this carousel
        count(*) as total_flights
        
    from {{ ref('int_flights') }}
    where flight_type = 'arrival'  -- Only arrivals have baggage data
      and baggage_claim_unit is not null
    group by baggage_claim_unit, airline_iata, airline_name, flight_date
)

select
    {{ dbt_utils.generate_surrogate_key(['baggage_claim_unit', 'airline_iata', 'flight_date']) }} as baggage_performance_key,
    *
from baggage_stats
order by baggage_claim_unit, airline_iata, flight_date
