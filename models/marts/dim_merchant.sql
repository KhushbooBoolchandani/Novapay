{{ config(materialized='table', schema='DIMENSIONAL') }}

select
    merchant_sk,
    merchant_id,
    merchant_name,
    merchant_category,
    country_code,
    is_active_flag

from {{ ref('stg_transactions__merchants') }}