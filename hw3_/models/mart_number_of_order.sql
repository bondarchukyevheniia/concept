with orders as (
    select * from {{ref("stg_orders")}}
)
select 
    order_id,
    user_id,
    order_date,
    row_number() over (partition by user_id order by order_date) as order_number
from orders