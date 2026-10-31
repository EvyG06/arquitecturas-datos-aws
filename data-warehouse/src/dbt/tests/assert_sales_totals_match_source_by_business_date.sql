-- Falla si el subtotal por fecha de negocio de fct_sales no coincide con la fuente validada.
with source_totals as (
    select
        cast(orders.order_date as date) as date,
        sum(items.subtotal) as subtotal
    from {{ ref('int_mysql__order_items__valid') }} as items
    inner join {{ ref('int_mysql__orders__valid') }} as orders using (order_id)
    group by 1
),
fact_totals as (
    select
        dates.date,
        sum(sales.subtotal) as subtotal
    from {{ ref('fct_sales') }} as sales
    inner join {{ ref('dim_date') }} as dates using (date_sk)
    group by 1
)
select
    coalesce(source_totals.date, fact_totals.date) as date,
    source_totals.subtotal as source_subtotal,
    fact_totals.subtotal as fact_subtotal
from source_totals
full outer join fact_totals using (date)
where source_totals.subtotal is distinct from fact_totals.subtotal
