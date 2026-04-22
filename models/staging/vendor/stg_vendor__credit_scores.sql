with source as (
    select * from {{ source('vendor_sources', 'credit_scores') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'score_date']) }} as credit_score_sk,
        customer_id,
        cast(score_date as date) as score_date,
        cast(credit_score as integer) as credit_score,
        risk_category,
        _LOADED_AT as loaded_at
    from source
)
select * from renamed
