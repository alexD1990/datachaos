with truth as (
    select order_id, event_type as status
    from {{ ref('stg_order_events') }}
    qualify row_number() over (partition by order_id order by event_time desc) = 1
)
select * from (select * from truth except select * from {{ ref('order_status_by_ingestion') }})
