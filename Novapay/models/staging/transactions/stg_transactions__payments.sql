{{ config(
    materialized = 'incremental',
    unique_key   = 'payment_id'
) }}

with payments_src as (

    select
        *
    from {{ source('transaction_sources', 'payments') }}

    {% if is_incremental() %}
        where _LOADED_AT >= (
            select dateadd(day, -3, max(loaded_at))
            from {{ this }}
        )
    {% endif %}

),

payments_clean as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'payments_src.payment_id',
            'payments_src.payment_timestamp'
        ]) }} as payment_sk,

        payments_src.payment_id,
        payments_src.customer_id,
        payments_src.account_id,
        payments_src.merchant_id,
        payments_src.payment_amount::number(18,2) as payment_amount,
        payments_src.fee_amount::number(18,2) as fee_amount,
        payments_src.payment_currency as currency_code,
        payments_src.payment_status   as status,
        payments_src.payment_date::date as payment_date,
        payments_src._LOADED_AT as loaded_at
    from payments_src

),

deduplicated as (
    select *
    from payments_clean
    qualify row_number() over (
        partition by payment_id
        order by loaded_at desc
    ) = 1
)
select
    *
from deduplicated