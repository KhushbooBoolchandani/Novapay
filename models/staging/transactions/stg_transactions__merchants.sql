with merchants_src as (

    select
        *
    from {{ source('transaction_sources', 'merchants') }}

),

merchants_clean as (

    select
        {{ dbt_utils.generate_surrogate_key(['merchants_src.merchant_id']) }} as merchant_sk,
        merchants_src.merchant_id,
        merchants_src.merchant_name,
        merchants_src.merchant_category,
        merchants_src.merchant_country as country_code,
        merchants_src.is_active::boolean as is_active_flag,
        merchants_src._LOADED_AT as loaded_at

    from merchants_src

)

select
    *
from merchants_clean