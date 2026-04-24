with customer_src as (

    select
        *
    from {{ source('crm_sources', 'customers') }}

),

customer_clean as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_src.raw_data:customer_id']) }} as customer_sk,

        customer_src.raw_data:customer_id::varchar        as customer_id,
        customer_src.raw_data:first_name::varchar         as first_name,
        customer_src.raw_data:last_name::varchar          as last_name,
        customer_src.raw_data:email::varchar              as email,
        customer_src.raw_data:phone::varchar              as phone_number,
        customer_src.raw_data:date_of_birth::date         as birth_date,
        customer_src.raw_data:address:city::varchar       as city,
        customer_src.raw_data:address:country::varchar    as country_code,
        customer_src.raw_data:customer_since::date        as joined_date,
        customer_src.raw_data:is_active::boolean          as is_active_flag,
        customer_src._LOADED_AT                           as loaded_at

    from customer_src

)

select
    *
from customer_clean