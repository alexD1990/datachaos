{{ config(materialized='incremental', unique_key='order_id') }}

-- Vulnerable to a late parent: an order that lands before its customer falls out of the inner join,
-- and the watermark on ingested_at means that it is never looked at again.
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
{% if is_incremental() %}
where orders.ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
