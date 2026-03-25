with users as ( select * from {{ref("stg_users")}}),
     orders as ( select * from {{ref("stg_orders")}} where status = 'completed')
     
select u.first_name, count(o.order_id) as completed_orders
from users u
join orders o on u.user_id = o.user_id
group by u.first_name 
order by completed_orders desc