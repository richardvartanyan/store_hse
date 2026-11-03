--ФАЙЛ

--варн т.к. есть заказ с двойным списанием. надо это заметить. Варн говорит дбт сообщить о проблеме, но не выкидывать ошибку
{{ config(severity='warn') }}

select
    order_id,
    order_amount,
    paid_amount
from {{ ref('fct_orders') }}
where is_confirmed = true and paid_amount > order_amount