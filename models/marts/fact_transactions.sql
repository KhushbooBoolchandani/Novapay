{{ config(materialized='table', schema='DIMENSIONAL') }}

with payments as (

    select * from {{ ref('stg_transactions__payments') }}

),

final as (

    select
        payment_sk,

        customer_id,
        account_id,
        merchant_id,

        payment_amount,
        payment_date,
        status

    from payments

)

select * from final