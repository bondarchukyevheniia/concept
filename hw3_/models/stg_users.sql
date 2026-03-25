{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

with user as (
    select * from {{ ref('raw_users') }}
)

select * from user

{% if is_incremental() %}
    where registration_date > (select max(registration_date) from {{ this }})
{% endif %}