-- Two rows per order are fine after a historical update, but not with the same updated_at.
select order_id, updated_at, count(*) as versions
from {{ source('landing', 'orders') }}
group by 1, 2
having count(*) > 1
