{# What the daily models and their full recalculation report per day. #}
{% macro daily_order_metrics() %}
    count(*) as orders,
    sum(amount) as amount_sum,
    count(*) filter (where status = 'created') as orders_created,
    count(*) filter (where status = 'paid') as orders_paid,
    count(*) filter (where status = 'shipped') as orders_shipped,
    count(*) filter (where status = 'delivered') as orders_delivered,
    count(*) filter (where status = 'cancelled') as orders_cancelled
{% endmacro %}

{# Rows that differ between an incremental daily model and a full recalculation, in either way. #}
{% macro daily_orders_differences(model) %}
with full_recalculation as (
    select
        cast(order_timestamp as date) as order_date,
        {{ daily_order_metrics() }}
    from {{ ref('stg_orders') }}
    group by 1
),
incremental as (
    select
        order_date,
        orders,
        amount_sum,
        orders_created,
        orders_paid,
        orders_shipped,
        orders_delivered,
        orders_cancelled
    from {{ ref(model) }}
)
select * from (select * from full_recalculation except select * from incremental)
union all
select * from (select * from incremental except select * from full_recalculation)
{% endmacro %}
