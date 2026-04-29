{{ config(materialized='table', schema='DIMENSIONAL') }}

with date_spine as (

    select
        dateadd(day, seq4(), '2020-01-01') as date_day
    from table(generator(rowcount => 2557))  -- ~7 years

),

final as (

    select
        date_day,

        year(date_day) as year,
        month(date_day) as month,
        day(date_day) as day,

        quarter(date_day) as quarter,

        case 
            when dayofweek(date_day) in (0,6) then true
            else false
        end as is_weekend,

        concat('Q', quarter(date_day)) as fiscal_quarter

    from date_spine

)

select * from final