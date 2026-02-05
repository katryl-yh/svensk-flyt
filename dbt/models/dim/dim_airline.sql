{{
    config(
        materialized='table'
    )
}}

with base as (
    select
        airline_iata,
        nullif(trim(airline_name), '') as airline_name,
        flight_date
    from {{ ref('int_flights') }}
    where airline_iata is not null
),

rolled as (
    select
        airline_iata,
        airline_name,
        max(flight_date) as max_flight_date,
        count(*) as flight_count
    from base
    group by airline_iata, airline_name
),

ranked as (
    select
        airline_iata,
        airline_name,
        row_number() over (
            partition by airline_iata
            order by
                case when airline_name is null then 1 else 0 end,
                max_flight_date desc,
                flight_count desc,
                airline_name
        ) as rn
    from rolled
)

select
    {{ dbt_utils.generate_surrogate_key(['airline_iata']) }} as airline_key,
    airline_iata,
    airline_name
from ranked
where rn = 1
