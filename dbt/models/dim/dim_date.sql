{{
    config(
        materialized='table'
    )
}}

-- Date dimension with pre-computed attributes
-- Covers 2020-2030 for historical and future flight data

with date_spine as (
    select
        date_add(date '2020-01-01', interval (row_number() over () - 1) day) as date_day
    from (
        -- Generate enough rows for 10+ years (3650+ days)
        select 1 as n from (values (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) as t1(n)
        cross join (values (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) as t2(n)
        cross join (values (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) as t3(n)
        cross join (values (1),(1),(1),(1)) as t4(n)
    ) as numbers
),

date_attributes as (
    select
        date_day,
        
        -- Date key (integer format: YYYYMMDD)
        cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
        
        -- Year attributes
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        
        -- Month attributes
        extract(month from date_day) as month,
        strftime(date_day, '%B') as month_name,
        strftime(date_day, '%b') as month_name_short,
        
        -- Week attributes
        extract(week from date_day) as week_number,
        date_trunc('week', date_day) as week_start_date,
        
        -- Day attributes
        extract(day from date_day) as day_of_month,
        extract(isodow from date_day) as day_of_week,  -- 1=Monday, 7=Sunday
        strftime(date_day, '%A') as day_name,
        strftime(date_day, '%a') as day_name_short,
        
        -- Boolean flags
        case when extract(isodow from date_day) in (6, 7) then true else false end as is_weekend,
        
        -- Fiscal year (if different from calendar, adjust here)
        extract(year from date_day) as fiscal_year,
        
        -- Relative date helpers
        case 
            when date_day = current_date then true 
            else false 
        end as is_today,
        
        case 
            when date_day between date_trunc('month', current_date) and current_date 
            then true 
            else false 
        end as is_current_month,
        
        case 
            when date_day between date_trunc('year', current_date) and current_date 
            then true 
            else false 
        end as is_current_year
        
    from date_spine
    where date_day <= date '2030-12-31'
)

select * from date_attributes
order by date_day
