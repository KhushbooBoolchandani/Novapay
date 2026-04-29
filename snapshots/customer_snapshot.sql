{% snapshot customer_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_tier', 'address', 'is_active_flag']
    )
}}

select *
from {{ ref('stg_crm__customers') }}

{% endsnapshot %}