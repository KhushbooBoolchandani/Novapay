with source as (
    select * from {{ source('transaction_sources', 'merchants') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['merchant_id']) }} as merchant_sk,
        merchant_id,
        merchant_name,
        merchant_category,
        merchant_country as country_code,
        is_active::boolean as is_active_flag,
        _LOADED_AT as loaded_at
    from source
)
select * from renamed
