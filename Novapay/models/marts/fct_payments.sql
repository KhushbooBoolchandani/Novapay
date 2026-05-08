{{ config(
    materialized='incremental',
    unique_key='payment_id',
    schema='DIMENSIONAL'
) }}

with source as (

    select * from {{ ref('int_fct_payments') }}

    {% if is_incremental() %}
        where loaded_at >= (
            select dateadd(day, -3, max(loaded_at)) from {{ this }}
        )
    {% endif %}

),

final as (

    select
        payment_sk,
        payment_id,

        customer_sk,
        account_sk,
        merchant_sk,

        payment_date as date_key,

        payment_amount,
        fee_amount,

        (payment_amount - fee_amount) as net_amount,

    
        case 
            when payment_amount < 0 then true
            else false
        end as is_refund,

        currency_code,
        status,

        loaded_at

    from source

)

select * from final