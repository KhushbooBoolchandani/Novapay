with credit_src as (

    select 
        *
    from {{ source('vendor_sources', 'credit_scores') }}

),

credit_transformed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'credit_src.customer_id',
            'credit_src.score_date'
        ]) }} as credit_score_sk,
        credit_src.customer_id                                as customer_id,
        credit_src.score_date::date                           as score_date,
        credit_src.credit_score::int                          as credit_score,
        credit_src.risk_category                              as risk_category,
        credit_src._LOADED_AT                                 as loaded_at
    from credit_src

)

select 
    *
from credit_transformed