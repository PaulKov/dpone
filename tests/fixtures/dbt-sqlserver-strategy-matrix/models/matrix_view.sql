{{ config(
    materialized='view',
    contract={'enforced': true},
    as_columnstore=false,
    indexes=[]
) }}

select
    cast(id as int) as id,
    cast(payload as varchar(32)) as payload,
    cast(batch_id as int) as batch_id
from {{ adapter.quote(var('source_schema')) }}.{{ adapter.quote(var('source_table')) }}
