with source as (
    select * from {{ source('crm_sources', 'customers') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['raw_data:customer_id']) }} as customer_sk,
        raw_data:customer_id::varchar as customer_id,
        raw_data:first_name::varchar as first_name,
        raw_data:last_name::varchar as last_name,
        raw_data:email::varchar as email,
        raw_data:phone::varchar as phone_number,
        raw_data:date_of_birth::date as birth_date,
        raw_data:address:city::varchar as city,
        raw_data:address:country::varchar as country_code,
        raw_data:customer_since::date as joined_date,
        raw_data:is_active::boolean as is_active_flag,
        _LOADED_AT as loaded_at
    from source
)
select * from renamed
