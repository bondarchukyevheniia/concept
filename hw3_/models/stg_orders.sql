{{config(
    materialized='incremental',
    unique_key='order_id'
)}}

with orders as (
    select * from {{ref("raw_orders")}}
)
select * from orders

{% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
{% endif %}