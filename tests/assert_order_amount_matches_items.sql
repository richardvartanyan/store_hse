--ФАЙЛ

--сумма товаров в каждом заказе
with items_sum as (
    select
        order_id,
        sum(line_amount) as total_items
    from {{ ref('fct_order_items') }}
    group by order_id
)

--беру суммы из таблы заказов и джоиню резы так, чтобы оставались только исключения\ошибки
select
    o.order_id,
    o.order_amount,
    i.total_items
from {{ ref('fct_orders')}} as o
left join items_sum as i on o.order_id = i.order_id
where i.total_items is null  or abs(o.order_amount - i.total_items) > 0.01