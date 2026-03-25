{{ config(materialized='view') }}

select * from {{ ref('raw_order_item') }}