{{ config(materialized='table', schema='DIMENSIONAL') }}
with customers as (

    select * from {{ ref('stg_crm__customers') }}

),

latest_credit_score as (

    select *
    from (
        select *,
               row_number() over (
                   partition by customer_id
                   order by score_date desc
               ) as rn
        from {{ ref('stg_vendor__credit_scores') }}
    )
    where rn = 1

),

final as (

    select
        c.customer_sk,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.city,
        c.country_code,
        c.joined_date,
        c.is_active_flag,

        cs.credit_score,
        cs.risk_category

    from customers c
    left join latest_credit_score cs
        on c.customer_id = cs.customer_id

)

select * from final