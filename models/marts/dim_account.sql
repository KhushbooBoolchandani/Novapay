{{ config(materialized='table', schema='DIMENSIONAL') }}

select
    account_sk,
    account_id,
    customer_id,
    account_type,
    account_status,
    current_balance,
    opened_date,
    closed_date

from {{ ref('stg_transactions__accounts') }}