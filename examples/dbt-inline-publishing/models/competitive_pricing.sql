{{ config(
    materialized='table',
    contract={'enforced': true},
    meta={
        'dpone': {
            'publish': {
                'enabled': true,
                'profile': 'mssql_to_clickhouse_mart',
                'workflow': 'competitive_pricing',
                'target': {
                    'schema': 'DWH_Stage',
                    'table': 'competitive_pricing'
                },
                'strategy': {
                    'mode': 'partition_replace',
                    'partition_key': 'date_id',
                    'window_days': 45
                },
                'physical_design': {
                    'profile': 'replicated_mart',
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

select
    cast(
        convert(
            datetime2,
            '{{ var("dpone_data_interval_start", "2026-01-01T00:00:00Z") }}',
            127
        ) as date
    ) as date_id,
    cast(product_id as bigint) as product_id,
    cast(calculated_price as decimal(18, 2)) as calculated_price,
    cast(price_source as nvarchar(100)) as price_source
from {{ source('pricing_raw', 'competitive_price_inputs') }}
