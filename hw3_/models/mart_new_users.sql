{{config(materialized='incremental', 
unique_key='registration_date')}}

select registration_date, count(user_id) as new_users
from {{ref("stg_users")}}

{% if is_incremental() %}
    where registration_date > (select max(registration_date) from {{ this }})
{% endif %}
group by registration_date