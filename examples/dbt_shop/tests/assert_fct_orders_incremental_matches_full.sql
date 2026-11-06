with incremental as (
    select order_id, status, amount from {{ ref('fct_orders_incremental') }}
),
full_table as (
    select order_id, status, amount from {{ ref('fct_orders') }}
)
select * from (select * from full_table except select * from incremental)
union all
select * from (select * from incremental except select * from full_table)
