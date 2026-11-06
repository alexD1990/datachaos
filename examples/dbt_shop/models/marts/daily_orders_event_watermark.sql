{{ config(materialized='incremental', unique_key='order_date') }}

-- Vulnerable: recalculates only from the day of the watermark on event time. The 30 minutes cover the
-- usual ingest delay, so that normal data stays right. Late rows and new versions of old orders
-- belong to days it never looks at again.
select
    cast(order_timestamp as date) as order_date,
    {{ daily_order_metrics() }},
    max(order_timestamp) as last_order_timestamp
from {{ ref('stg_orders') }}
{% if is_incremental() %}
where cast(order_timestamp as date) >= cast(
    (select max(last_order_timestamp) from {{ this }}) - interval 30 minute as date
)
{% endif %}
group by 1
