{{ config(materialized='incremental', 
unique_key='order_date') }}

select order_date, count(order_id) as completed_orders_count
from {{ ref("stg_orders")}}
where status = 'completed'

{% if is_incremental() %}
    and order_date > (select max(order_date) from {{ this }})
{% endif %}
group by order_date