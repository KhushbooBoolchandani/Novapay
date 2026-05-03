{{ config(materialized='view') }}

with payments as (

    select * from {{ ref('stg_transactions__payments') }}

),

customers as (

    select customer_id, customer_sk
    from {{ ref('dim_customer') }}

),

accounts as (

    select account_id, account_sk
    from {{ ref('dim_account') }}

),

merchants as (

    select merchant_id, merchant_sk
    from {{ ref('dim_merchant') }}

),

final as (

    select
        p.payment_id,
        p.payment_sk,

        c.customer_sk,
        a.account_sk,
        m.merchant_sk,

        p.payment_date,
        p.payment_amount,
        p.fee_amount,

        p.currency_code,
        p.status,

        p.loaded_at

    from payments p
    left join customers c on p.customer_id = c.customer_id
    left join accounts a on p.account_id = a.account_id
    left join merchants m on p.merchant_id = m.merchant_id

)

select * from final