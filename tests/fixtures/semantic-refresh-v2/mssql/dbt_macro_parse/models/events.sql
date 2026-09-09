{{ config(
    materialized='incremental',
    incremental_strategy='dpone_scope_merge',
    unique_key=['event_id', 'occurred_at'],
    on_schema_change='fail',
    contract={'enforced': true},
    as_columnstore=false,
    indexes=[]
) }}

select
    cast(1 as bigint) as event_id,
    cast('2026-08-08T00:00:00.000000' as datetime2(6)) as occurred_at,
    cast('payload' as varchar(32)) as payload
