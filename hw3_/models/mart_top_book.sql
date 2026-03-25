with order_item as ( select * from {{ref("stg_order_item")}}),
     books as ( select * from {{ref("stg_books")}}),
     orders as ( select * from {{ ref("stg_orders")}} where status = 'completed')
     
select b.title, sum(b.price * i.quantity) as revenue
from order_item i
join books b on i.book_id = b.book_id
join orders o on i.order_id = o.order_id
group by b.title 
order by revenue desc