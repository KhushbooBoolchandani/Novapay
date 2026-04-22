with accounts_src as (

    select
        *
    from {{ source('transaction_sources', 'accounts') }}

),

accounts_clean as (

    select
        {{ dbt_utils.generate_surrogate_key(['accounts_src.account_id']) }} as account_sk,

        accounts_src.account_id,
        accounts_src.customer_id,
        accounts_src.account_type,
        accounts_src.account_status,
        accounts_src.current_balance::number(18,2) as current_balance,
        accounts_src.opening_date::date as opened_date,
        accounts_src.closing_date::date as closed_date,
        accounts_src._LOADED_AT as loaded_at

    from accounts_src

)

select
    *
from accounts_clean