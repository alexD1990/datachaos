select
    orders.order_id,
    orders.customer_id,
    customers.country_code,
    orders.status,
    orders.amount,
    orders.currency,
    orders.order_timestamp,
    orders.ingested_at
from {{ ref('stg_orders') }} as orders
inner join {{ ref('stg_customers') }} as customers using (customer_id)
