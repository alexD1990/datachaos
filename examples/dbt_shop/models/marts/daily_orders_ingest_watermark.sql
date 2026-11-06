{{ config(materialized='incremental', unique_key='order_date') }}

-- Robust: finds the days touched by rows ingested after the watermark, and recalculates each of them
-- as a whole, so that a late row or a new version of an old order corrects the day it belongs to.
{% if is_incremental() %}
with touched_days as (
    select distinct cast(order_timestamp as date) as order_date
    from {{ ref('stg_orders') }}
    where ingested_at > (select max(last_ingested_at) from {{ this }})
)
{% endif %}
select
    cast(order_timestamp as date) as order_date,
    {{ daily_order_metrics() }},
    max(ingested_at) as last_ingested_at
from {{ ref('stg_orders') }}
{% if is_incremental() %}
where cast(order_timestamp as date) in (select order_date from touched_days)
{% endif %}
group by 1
