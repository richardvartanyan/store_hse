-- НОВЫЙ
{{ config(
    materialized='table',
    distributed_replicated=true
) }}



with dates_range as (
    select
        min(order_date) as start_date,
        max(order_date) as end_date
    from {{ ref('fct_orders') }}
)
--извлекаю конкретные части даты (isodow - день недели цифрой)
--латерал позволяет generate_series использовать даты из dates_range
select
    d.date_day::date as date_day,
    extract(year from d.date_day)::int as year,
    extract(quarter from d.date_day)::int as quarter,
    extract(month from d.date_day)::int as month,
    extract(isodow from d.date_day)::int as day_of_week,
    extract(isodow from d.date_day) in (6, 7) as is_weekend
from dates_range r
cross join lateral generate_series(
    r.start_date,
    r.end_date,
    interval '1 day'
) as d(date_day)