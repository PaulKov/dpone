{{ config(materialized='view') }}

select
    cast(1 as int) as order_id,
    cast(10.00 as decimal(18, 2)) as amount
