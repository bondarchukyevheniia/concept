with users as ( select * from {{ref("stg_users")}}),
     orders as ( select * from {{ref("stg_orders")}} where status = 'completed'),
     items as ( select * from {{ref("stg_order_item")}}),
     books as ( select * from {{ref("stg_books")}})

select u.city, sum((b.price - b.cost) * i.quantity) as profit
from users u
join orders o on u.user_id = o.user_id
join items i on o.order_id = i.order_id
join books b on i.book_id = b.book_id
group by u.city 
order by profit desc