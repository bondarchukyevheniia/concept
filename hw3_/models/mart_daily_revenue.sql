with order_item as (select * from {{ref("stg_order_item")}}),
     books as (select * from {{ref("stg_books")}}),
     orders as (select * from {{ref("stg_orders")}} where status = 'completed')

select o.order_date, sum(b.price * i.quantity) as daily_revenue
from orders o
join order_item i on o.order_id = i.order_id
join books b on i.book_id = b.book_id
group by o.order_date 
order by o.order_date