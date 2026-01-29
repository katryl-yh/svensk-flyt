{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('flights', 'flights_arrivals_raw') }}
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
        departure_airport_swedish as origin_airport_swedish,
        departure_airport_english as origin_airport_english,
        
        -- Timestamps (UTC)
        arrival_time__scheduled_utc as scheduled_arrival_utc,
        arrival_time__estimated_utc as estimated_arrival_utc,
        arrival_time__actual_utc as actual_arrival_utc,
        
        -- Location and status
        location_and_status__flight_leg_status as flight_status,
        location_and_status__terminal as terminal,
        location_and_status__gate as gate,
        
        -- Baggage information
        baggage__baggage_claim_unit as baggage_claim_unit,
        baggage__first_bag_utc as first_bag_utc,
        baggage__last_bag_utc as last_bag_utc,
        
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
        case when flight_status = 'LAN' then true else false end as is_landed,
        
        -- Domestic vs International
        case 
            when origin_airport_iata in ('ARN', 'BMA', 'GOT', 'MMX', 'LLA', 'UME', 'OSD', 'VBY', 'RNB', 'KRN') 
            then true 
            else false 
        end as is_domestic,
        
        -- Delay calculation (only for landed flights with actual times)
        case 
            when actual_arrival_utc is not null and scheduled_arrival_utc is not null
            then extract(epoch from (actual_arrival_utc - scheduled_arrival_utc)) / 60.0
            else null
        end as delay_minutes,
        
        -- Time dimensions
        extract(hour from scheduled_arrival_utc) as arrival_hour,
        extract(isodow from scheduled_arrival_utc) as arrival_day_of_week,
        strftime(scheduled_arrival_utc, '%A') as arrival_day_name,
        date_trunc('day', scheduled_arrival_utc) as arrival_date,
        
        -- Time period classification
        case 
            when extract(hour from scheduled_arrival_utc) between 6 and 11 then 'Morning (06:00-11:59)'
            when extract(hour from scheduled_arrival_utc) between 12 and 16 then 'Midday/Afternoon (12:00-16:59)'
            when extract(hour from scheduled_arrival_utc) between 17 and 21 then 'Evening (17:00-21:59)'
            else 'Night/Red-eye (22:00-05:59)'
        end as arrival_time_period,
        
        -- Punctuality flag (on-time = within 15 minutes)
        case 
            when actual_arrival_utc is not null 
                and scheduled_arrival_utc is not null
                and extract(epoch from (actual_arrival_utc - scheduled_arrival_utc)) / 60.0 <= 15
            then true
            else false
        end as is_on_time,
        
        -- Baggage handling time (if available)
        case 
            when first_bag_utc is not null and last_bag_utc is not null
            then extract(epoch from (last_bag_utc - first_bag_utc)) / 60.0
            else null
        end as baggage_handling_minutes
        
    from renamed
)

select * from calculated
