with daily_orders as (
    select order_date, count(order_id) as total_orders
    from {{ref("stg_orders")}}
    group by order_date
)
select order_date, 
total_orders, 
sum(total_orders) over (order by order_date) as total_daily_orders
from daily_orders