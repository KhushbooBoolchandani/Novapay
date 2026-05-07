{{ config(
    materialized='table',
    schema='DATA_VAULT'
) }}

{{ automate_dv.link(
    src_pk='customer_account_hk',
    src_fk=[
        'customer_hk',
        'account_hk'
    ],
    src_ldts='loaded_at',
    src_source='record_source',
    source_model='stg_dv_accounts'
) }}