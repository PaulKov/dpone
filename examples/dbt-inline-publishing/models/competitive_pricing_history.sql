{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['product_id', 'date_id'],
    on_schema_change='fail',
    contract={'enforced': true},
    meta={
        'dpone': {
            'publish': {
                'enabled': true,
                'profile': 'mssql_to_clickhouse_mart',
                'workflow': 'competitive_pricing',
                'strategy': {'mode': 'auto'},
                'physical_design': {
                    'order_by': ['product_id', 'date_id']
                },
                'execution': {
                    'profile': 'weak_worker',
                    'max_parallelism': 2
                },
                'quality': {'preset': 'strict'},
                'lineage': {'enabled': true}
            }
        }
    }
) }}

select date_id, product_id, calculated_price, price_source
from {{ ref('competitive_pricing') }}

{% if is_incremental() %}
where date_id >= cast(
    convert(
        datetime2,
        '{{ var("dpone_data_interval_start", "2026-01-01T00:00:00Z") }}',
        127
    ) as date
)
and date_id < cast(
    convert(
        datetime2,
        '{{ var("dpone_data_interval_end", "2026-01-02T00:00:00Z") }}',
        127
    ) as date
)
{% endif %}
