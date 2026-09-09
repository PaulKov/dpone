{% set source_schema = var('source_schema') %}
{% set source_table = var('source_table') %}

select
    source.*,
    cast(source.amount * cast(2 as decimal(18, 4)) as decimal(38, 8)) as dbt_calculated_amount
from {{ adapter.quote(source_schema) }}.{{ adapter.quote(source_table) }} as source
