{{
    config(
        materialized='table'
    )
}}

with airlines as (
    select distinct
        airline_iata,
        airline_name
    from {{ ref('int_flights') }}
    where airline_iata is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['airline_iata']) }} as airline_key,
    airline_iata,
    airline_name
from airlines
