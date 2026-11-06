select
    event_id,
    order_id,
    event_type,
    cast(event_time as timestamp) as event_time,
    cast(ingested_at as timestamp) as ingested_at
from {{ source('landing', 'order_events') }}
qualify row_number() over (partition by event_id order by ingested_at desc) = 1
