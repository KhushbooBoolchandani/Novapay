select *
from {{ ref('stg_transactions__payments') }}
where status = 'completed'
  and payment_amount < 0