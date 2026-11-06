select
    customer_id,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    country_code,
    customer_segment,
    cast(is_active as boolean) as is_active,
    cast(ingested_at as timestamp) as ingested_at
from {{ source('landing', 'customers') }}
qualify row_number() over (partition by customer_id order by updated_at desc, ingested_at desc) = 1
