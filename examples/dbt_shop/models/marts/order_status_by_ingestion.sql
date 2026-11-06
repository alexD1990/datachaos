-- Vulnerable: the event that landed last wins, which assumes that events land in order.
select order_id, event_type as status
from {{ ref('stg_order_events') }}
qualify row_number() over (partition by order_id order by ingested_at desc) = 1
