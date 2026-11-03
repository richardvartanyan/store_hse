-- НОВЫЙ ФАЙД
{{ config(
    materialized='table', 
    distributed_by='order_id', 
    orientation='column'
) }}--cоздаю таблицу реальную, след стр - критерий сегментации


select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    o.customer_id,
    o.order_date,
    oi.quantity,
    oi.unit_price,
    oi.discount_amount,
    oi.line_amount,
    o.is_confirmed

from {{ ref('stg_shop__order_items') }} as oi
join {{ ref('fct_orders') }} as o -- Обычный джоин, чтобы тестовые заказы убрать(которых нет в фкт_ордерс)
    on oi.order_id = o.order_id

-- docker compose run --rm dbt run -s fct_order_items 