select event_id, occurred_at from {{ source('raw', 'events') }}
