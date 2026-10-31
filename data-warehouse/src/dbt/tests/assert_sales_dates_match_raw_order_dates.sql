-- Falla si la fecha de venta de fct_sales no coincide con la fecha del pedido en raw (sin transformar).
-- Cubre pedidos cercanos a medianoche: una conversión de zona horaria incorrecta cambiaría el día.
with raw_order_dates as (
    select
        order_id,
        cast(order_date as date) as date
    from {{ source('mysql_raw', 'mysql_orders') }}
),
expected as (
    select
        {{ dbt_utils.generate_surrogate_key(['cast(items.order_item_id as varchar)']) }} as sale_sk,
        dates.date_sk
    from {{ ref('int_mysql__order_items__valid') }} as items
    inner join raw_order_dates as orders using (order_id)
    inner join {{ ref('dim_date') }} as dates on dates.date = orders.date
)
select
    expected.sale_sk,
    expected.date_sk as expected_date_sk,
    sales.date_sk as fact_date_sk
from expected
inner join {{ ref('fct_sales') }} as sales using (sale_sk)
where expected.date_sk is distinct from sales.date_sk
