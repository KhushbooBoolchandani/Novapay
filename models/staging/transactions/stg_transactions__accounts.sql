with source as (
    select * from {{ source('transaction_sources', 'accounts') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_sk,
        account_id,
        customer_id,
        account_type,
        account_status,
        cast(current_balance as number(18,2)) as current_balance,
        cast(opening_date as date) as opened_date,
        cast(closing_date as date) as closed_date,
        _LOADED_AT as loaded_at
    from source
)
select * from renamed
