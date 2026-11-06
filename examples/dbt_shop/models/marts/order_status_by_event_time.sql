-- Robust: the event that happened last wins.
select order_id, event_type as status
from {{ ref('stg_order_events') }}
qualify row_number() over (partition by order_id order by event_time desc) = 1
