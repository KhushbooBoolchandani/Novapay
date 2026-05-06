with product_src as (

    select 
        *
    from {{ source('vendor_sources', 'product_catalog') }}

),

product_clean as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'product_src.product_id'
        ]) }} as product_sk,
        product_src.product_id                         as product_id,
        product_src.product_name                       as product_name,
        product_src.product_category                   as product_category,
        product_src.monthly_fee::number(18,2)          as monthly_fee,
        product_src.is_active::boolean                 as is_active_flag,
        product_src._LOADED_AT                         as loaded_at
    from product_src

)
select 
    *
from product_clean
