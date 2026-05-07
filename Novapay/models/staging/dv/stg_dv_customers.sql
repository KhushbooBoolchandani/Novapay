

{{ automate_dv.stage(
    include_source_columns=true,

    source_model='stg_crm__customers',

    derived_columns={
        'RECORD_SOURCE': '!CRM_SYSTEM',
        'LOAD_DATETIME': 'loaded_at'
    },

    hashed_columns={

        'CUSTOMER_HK': 'customer_id',

        'CUSTOMER_HASHDIFF': {
            'is_hashdiff': true,
            'columns': [
                'first_name',
                'last_name',
                'email',
                'city',
                'country_code',
                'is_active_flag'
            ]
        }

    }
) }}