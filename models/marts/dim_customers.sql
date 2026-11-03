-- CОЗДАЛ ФАЙЛ
{{ config(
    materialized='table',
    distributed_replicated=true
) }}


with customers as (
    select *
    from {{ ref('stg_shop__customers') }}
),

confirmed_orders as (
    select *
    from {{ ref('fct_orders') }}
    where is_confirmed = true --убираем негарант покупки
), 




order_stats as (
    select
        customer_id,
        count(*) as confirmed_orders_count,
        coalesce(sum(order_amount), 0) as confirmed_revenue
    from confirmed_orders
    group by customer_id
), --конкретно группирует второй СТе, мы получаем клиента, число его заказов и сумму его заказов финальную


first_orders as (
    select
        customer_id,
        order_id as first_confirmed_order_id,
        ordered_at as first_confirmed_at
    from (
        select
            customer_id,
            order_id,
            ordered_at,
            row_number() over (
                partition by customer_id
                order by ordered_at, order_id
            ) as rn
        from confirmed_orders
    ) as t
    where rn = 1
) --номируются(rn) заказы по каждому клиенту отдельно, сортируются по дате заказа и айди(ну если дата одинаковое будет двух). В итоге только 1ый берется рн = 1



select
    c.customer_id,
    c.registered_at,
    c.registration_date,
    c.city,
    c.acquisition_channel,
    f.first_confirmed_order_id,
    f.first_confirmed_at,
    coalesce(s.confirmed_orders_count, 0) as confirmed_orders_count,
    coalesce(s.confirmed_revenue, 0) as confirmed_revenue,
    coalesce(s.confirmed_orders_count, 0) > 0 as is_buyer

from customers as c
left join order_stats as s
    on c.customer_id = s.customer_id
left join first_orders as f
    on c.customer_id = f.customer_id