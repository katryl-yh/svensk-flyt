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
        r.*,
        
        -- Route key for grouping
        r.origin_airport_iata || '-' || r.destination_airport_iata as route_key,
        
        -- Flight type flags
        case when r.flight_status = 'DEL' then true else false end as is_deleted,
        case when r.flight_status = 'CAN' then true else false end as is_cancelled,
        case when r.flight_status = 'SCH' then true else false end as is_scheduled,
        
        -- Domestic vs International (both origin AND destination must be in Sweden)
        case 
            when coalesce(orig_seed.country, '') = 'Sweden' 
             and coalesce(dest_seed.country, '') = 'Sweden'
            then true 
            else false 
        end as is_domestic,
        
        -- Delay calculation (only for departed flights with actual times)
        case 
            when r.actual_departure_utc is not null and r.scheduled_departure_utc is not null
            then extract(epoch from (r.actual_departure_utc - r.scheduled_departure_utc)) / 60.0
            else null
        end as delay_minutes,
        
        -- Time dimensions
        extract(hour from r.scheduled_departure_utc) as departure_hour,
        extract(isodow from r.scheduled_departure_utc) as departure_day_of_week,
        strftime(r.scheduled_departure_utc, '%A') as departure_day_name,
        date_trunc('day', r.scheduled_departure_utc) as departure_date,
        
        -- Time period classification
        case 
            when extract(hour from r.scheduled_departure_utc) between 6 and 11 then 'Morning (06:00-11:59)'
            when extract(hour from r.scheduled_departure_utc) between 12 and 16 then 'Midday/Afternoon (12:00-16:59)'
            when extract(hour from r.scheduled_departure_utc) between 17 and 21 then 'Evening (17:00-21:59)'
            else 'Night/Red-eye (22:00-05:59)'
        end as departure_time_period,
        
        -- Punctuality flag (on-time = within 15 minutes)
        case 
            when r.actual_departure_utc is not null 
                and r.scheduled_departure_utc is not null
                and extract(epoch from (r.actual_departure_utc - r.scheduled_departure_utc)) / 60.0 <= 15
            then true
            else false
        end as is_on_time
        
    from renamed r
    left join {{ ref('airport_seed') }} orig_seed on r.origin_airport_iata = orig_seed.iata
    left join {{ ref('airport_seed') }} dest_seed on r.destination_airport_iata = dest_seed.iata
)

select * from calculated
qualify row_number() over (
    partition by flight_id, scheduled_departure_utc 
    order by _dlt_load_id desc
) = 1
