with order_item as ( select * from {{ref("stg_order_item")}}),
     books as ( select * from {{ref("stg_books")}}),
     orders as ( select * from {{ref("stg_orders")}} where status = 'completed')

select b.title, sum((b.price - b.cost) * i.quantity) as profit
from order_item i
join books b on i.book_id = b.book_id
join orders o on i.order_id = o.order_id
group by b.title 
order by profit desc