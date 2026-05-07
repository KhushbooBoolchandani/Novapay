
{{ automate_dv.stage(
    include_source_columns=true,

    source_model='stg_transactions__accounts',

    derived_columns={
        'RECORD_SOURCE': '!TRANSACTIONS_SYSTEM',
        'LOAD_DATETIME': 'loaded_at'
    },

    hashed_columns={

        'ACCOUNT_HK': 'account_id',

        'CUSTOMER_HK': 'customer_id',

        'CUSTOMER_ACCOUNT_HK': [
            'customer_id',
            'account_id'
        ],

        'ACCOUNT_HASHDIFF': {
            'is_hashdiff': true,
            'columns': [
                'account_type',
                'account_status',
                'current_balance',
                'opened_date',
                'closed_date'
            ]
        }

    }
) }}