select *
from {{ ref('stg_transactions__accounts') }}
where account_status = 'closed'
  and closed_date is null