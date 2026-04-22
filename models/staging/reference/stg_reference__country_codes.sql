with source as (
    select * from {{ source('reference_sources', 'country_codes') }}
),
renamed as (
    select
        country_code_2 as country_code,
        country_name,
        region,
        _LOADED_AT as loaded_at
    from source
)
select * from renamed
