{{
    config(
        materialized='table'
    )
}}

-- Auto-discover all airports from flight data
-- LEFT JOIN to seed file for names (only maintain names, not codes)
-- Unknown airports show as "⚠️ UPDATE SEED - [IATA]" to alert that the seed needs updating

with all_airports_from_data as (
    -- Extract all unique IATA codes from intermediate flights
    select distinct origin_airport_iata as airport_iata
    from {{ ref('int_flights') }}
    where origin_airport_iata is not null
    
    union
    
    select distinct destination_airport_iata as airport_iata
    from {{ ref('int_flights') }}
    where destination_airport_iata is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['a.airport_iata']) }} as airport_key,
    a.airport_iata,
    case 
        when s.airport_name is null or s.airport_name = '' 
        then '⚠️ UPDATE SEED - ' || a.airport_iata
        else s.airport_name
    end as airport_name,
    s.country
from all_airports_from_data a
left join {{ ref('airport_seed') }} s on a.airport_iata = s.iata

