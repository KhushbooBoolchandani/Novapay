{{ config(materialized='incremental', unique_key='payment_id') }}

with source as (
    select * from {{ source('transaction_sources', 'payments') }}
    {% if is_incremental() %}
      where _LOADED_AT >= (select dateadd(day, -3, max(loaded_at)) from {{ this }})
    {% endif %}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['payment_id', 'payment_timestamp']) }} as payment_sk,
        payment_id,
        customer_id,
        account_id,
        merchant_id,
        cast(payment_amount as number(18,2)) as payment_amount,
        payment_currency as currency_code,
        payment_status as status,
        cast(payment_date as date) as payment_date,
        _LOADED_AT as loaded_at
    from source
)
select * from renamed
qualify row_number() over (partition by payment_id order by loaded_at desc) = 1
