{{ config(
    materialized = 'incremental',
    unique_key   = 'interaction_id'
) }}

with interactions_src as (

    select
        *
    from {{ source('crm_sources', 'customer_interactions') }}

    {% if is_incremental() %}
        where _LOADED_AT >= (
            select dateadd(day, -3, max(loaded_at))
            from {{ this }}
        )
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['interactions_src.raw_data:interaction_id']) }} as interaction_sk,

        interactions_src.raw_data:interaction_id::varchar     as interaction_id,
        interactions_src.raw_data:customer_id::varchar        as customer_id,
        interactions_src.raw_data:interaction_type::varchar   as interaction_type,
        interactions_src.raw_data:interaction_date::date      as interaction_date,
        interactions_src.raw_data:status::varchar             as status,
        interactions_src.raw_data:priority::varchar           as priority,

        interactions_src._LOADED_AT                           as loaded_at

    from interactions_src

)

select *
from final