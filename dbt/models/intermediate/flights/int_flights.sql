{{
    config(
        materialized='view'
    )
}}

with arrivals as (
    select
        flight_id,
        flight_number,
        origin_airport_iata,
        destination_airport_iata,
        departure_date_utc,
        airline_name,
        airline_iata,
        scheduled_arrival_utc as scheduled_time_utc,
        actual_arrival_utc as actual_time_utc,
        flight_status,
        terminal,
        gate,
        route_key,
        is_deleted,
        is_cancelled,
        is_domestic,
        delay_minutes,
        arrival_hour as flight_hour,
        arrival_day_of_week as flight_day_of_week,
        arrival_day_name as flight_day_name,
        arrival_date as flight_date,
        arrival_time_period as flight_time_period,
        is_on_time,
        -- Arrival-specific fields
        is_landed,
        baggage_claim_unit,
        first_bag_utc,
        last_bag_utc,
        baggage_handling_minutes,
        -- Type discriminator
        'arrival' as flight_type
    from {{ ref('stg_flights_arrivals') }}
),

departures as (
    select
        flight_id,
        flight_number,
        origin_airport_iata,
        destination_airport_iata,
        departure_date_utc,
        airline_name,
        airline_iata,
        scheduled_departure_utc as scheduled_time_utc,
        actual_departure_utc as actual_time_utc,
        flight_status,
        terminal,
        gate,
        route_key,
        is_deleted,
        is_cancelled,
        is_domestic,
        delay_minutes,
        departure_hour as flight_hour,
        departure_day_of_week as flight_day_of_week,
        departure_day_name as flight_day_name,
        departure_date as flight_date,
        departure_time_period as flight_time_period,
        is_on_time,
        -- Departure-specific fields (set to NULL for union compatibility)
        null as is_landed,
        null as baggage_claim_unit,
        null::timestamp as first_bag_utc,
        null::timestamp as last_bag_utc,
        null::double as baggage_handling_minutes,
        -- Type discriminator
        'departure' as flight_type
    from {{ ref('stg_flights_departures') }}
),

unioned as (
    select * from arrivals
    union all
    select * from departures
)

select * from unioned
