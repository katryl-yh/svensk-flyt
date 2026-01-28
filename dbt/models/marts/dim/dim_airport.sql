{{
    config(
        materialized='table'
    )
}}

with swedish_airports as (
    select 'ARN' as airport_iata, 'Stockholm Arlanda Airport' as airport_name union all
    select 'BMA', 'Bromma Stockholm Airport' union all
    select 'GOT', 'Göteborg Landvetter Airport' union all
    select 'MMX', 'Malmö Airport' union all
    select 'LLA', 'Luleå Airport' union all
    select 'UME', 'Umeå Airport' union all
    select 'OSD', 'Åre Östersund Airport' union all
    select 'VBY', 'Visby Airport' union all
    select 'RNB', 'Ronneby Airport' union all
    select 'KRN', 'Kiruna Airport'
)

select
    {{ dbt_utils.generate_surrogate_key(['airport_iata']) }} as airport_key,
    airport_iata,
    airport_name
from swedish_airports
