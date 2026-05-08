{{ config(
    materialized='table',
    schema='DATA_VAULT'
) }}

{{ automate_dv.sat(
    src_pk='CUSTOMER_HK',
    src_hashdiff='HASHDIFF',
    src_payload=[
        'FIRST_NAME',
        'LAST_NAME',
        'EMAIL',
        'PHONE_NUMBER',
        'BIRTH_DATE',
        'JOINED_DATE',
        'CITY',
        'COUNTRY_CODE',
        'IS_ACTIVE_FLAG'
    ],
    src_ldts='LOADED_AT',
    src_source='RECORD_SOURCE',
    source_model='stg_dv_customers'
) }}