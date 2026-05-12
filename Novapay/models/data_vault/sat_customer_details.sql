{{ config(
    materialized='table',
    schema='DATA_VAULT'
) }}

{{ automate_dv.sat(
    src_pk='customer_hk',
    src_hashdiff='hashdiff',
    src_payload=[
        'first_name',
        'last_name',
        'email',
        'phone_number',
        'birth_date',
        'joined_date',
        'city',
        'country_code',
        'is_active_flag'
    ],
    src_ldts='loaded_at',
    src_source='record_source',
    source_model='stg_dv_customers'
) }}