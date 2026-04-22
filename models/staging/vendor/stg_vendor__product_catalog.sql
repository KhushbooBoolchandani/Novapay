with source as (
    select * from {{ source('vendor_sources', 'product_catalog') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
        product_id,
        product_name,
        product_category,
        cast(monthly_fee as number(18,2)) as monthly_fee,
        is_active::boolean as is_active_flag,
        _LOADED_AT as loaded_at
    from source
)
select * from renamed
