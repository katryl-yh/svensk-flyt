{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('flights', 'flights_departures_raw') }}
),

renamed as (
    select
        -- Primary identifiers
        flight_id,
        flight_leg_identifier__flight_id as flight_number,
        flight_leg_identifier__departure_airport_iata as origin_airport_iata,
        flight_leg_identifier__arrival_airport_iata as destination_airport_iata,
        flight_leg_identifier__flight_departure_date_utc as departure_date_utc,
        
        -- Airline information
        airline_operator__name as airline_name,
        airline_operator__iata as airline_iata,
        
        -- Airport names
        arrival_airport_swedish as destination_airport_swedish,
        arrival_airport_english as destination_airport_english,
        
        -- Timestamps (UTC)
        departure_time__scheduled_utc as scheduled_departure_utc,
        departure_time__estimated_utc as estimated_departure_utc,
        departure_time__actual_utc as actual_departure_utc,
        
        -- Location and status
        location_and_status__flight_leg_status as flight_status,
        location_and_status__terminal as terminal,
        location_and_status__gate as gate,
        
        -- Metadata
        _dlt_load_id,
        _dlt_id
    from source
),

calculated as (
    select
        *,
        
        -- Route key for grouping
        origin_airport_iata || '-' || destination_airport_iata as route_key,
        
        -- Flight type flags
        case when flight_status = 'DEL' then true else false end as is_deleted,
        case when flight_status = 'CAN' then true else false end as is_cancelled,
        case when flight_status = 'SCH' then true else false end as is_scheduled,
        
        -- Domestic vs International
        case 
            when destination_airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN') 
            then true 
            else false 
        end as is_domestic,
        
        -- Delay calculation (only for departed flights with actual times)
        case 
            when actual_departure_utc is not null and scheduled_departure_utc is not null
            then extract(epoch from (actual_departure_utc - scheduled_departure_utc)) / 60.0
            else null
        end as delay_minutes,
        
        -- Time dimensions
        extract(hour from scheduled_departure_utc) as departure_hour,
        extract(isodow from scheduled_departure_utc) as departure_day_of_week,
        strftime(scheduled_departure_utc, '%A') as departure_day_name,
        date_trunc('day', scheduled_departure_utc) as departure_date,
        
        -- Time period classification
        case 
            when extract(hour from scheduled_departure_utc) between 6 and 11 then 'Morning (06:00-11:59)'
            when extract(hour from scheduled_departure_utc) between 12 and 16 then 'Midday/Afternoon (12:00-16:59)'
            when extract(hour from scheduled_departure_utc) between 17 and 21 then 'Evening (17:00-21:59)'
            else 'Night/Red-eye (22:00-05:59)'
        end as departure_time_period,
        
        -- Punctuality flag (on-time = within 15 minutes)
        case 
            when actual_departure_utc is not null 
                and scheduled_departure_utc is not null
                and extract(epoch from (actual_departure_utc - scheduled_departure_utc)) / 60.0 <= 15
            then true
            else false
        end as is_on_time
        
    from renamed
)

select * from calculated
qualify row_number() over (
    partition by flight_id, scheduled_departure_utc 
    order by _dlt_load_id desc
) = 1
