select
    order_id,
    customer_id,
    cast(order_timestamp as timestamp) as order_timestamp,
    cast(updated_at as timestamp) as updated_at,
    status,
    cast(amount as decimal(18, 2)) as amount,
    currency,
    source_system,
    cast(ingested_at as timestamp) as ingested_at
from {{ source('landing', 'orders') }}
qualify row_number() over (partition by order_id order by updated_at desc, ingested_at desc) = 1
