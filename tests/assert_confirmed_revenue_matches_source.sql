--ФАЙЛ

--заказы с хоть одним успешным платежем
with source_payments as (
    select distinct order_id
    from {{ source('shop', 'payments') }}
    where payment_status = 'success'  and amount >= 0
),


--считаю выручку из исходных табл, беря ток оплаченные нетестовые заказы с оплатой
source_revenue as (
    select
        sum(o.total_amount) as revenue
    from {{ source('shop', 'orders') }} as o
    join source_payments as  p on o.order_id = p.order_id
    where o.status = 'paid' 
),

--туже выручку из фкт ордерс
mart_revenue as (
    select sum(order_amount) as revenue
    from {{ ref('fct_orders') }}
    where is_confirmed =true
)

--сравниваю одно с другим
select
    s.revenue as source_revenue,
    m.revenue as mart_revenue
from source_revenue as  s
cross join mart_revenue as m
where abs(s.revenue - m.revenue) > 0.01