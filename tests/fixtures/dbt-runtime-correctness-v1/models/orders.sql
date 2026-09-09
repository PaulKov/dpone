{{
    config(
        materialized='table',
        contract={'enforced': true}
    )
}}

select order_id, amount
from {{ ref('ephemeral_orders') }}
