{# Tabla de consumo del dashboard de QuickSight: una fila por linea de venta con sus dimensiones ya resueltas.
   La categoria es la de la version del producto vigente en la fecha de la venta (SCD2), no la actual.
   Es la unica tabla que puede leer bi_lector, el usuario de QuickSight (minimo privilegio). #}
{{ config(
    materialized='table',
    grants=({'select': ['bi_lector']} if target.type == 'redshift' else {})
) }}

select
    sales.sale_sk,
    dates.date as fecha,
    dates.year as anio,
    dates.month as mes,
    products.category as categoria,
    products.name as producto,
    channels.sales_channel as canal,
    geography.department as departamento,
    geography.city as ciudad,
    sales.quantity as cantidad,
    sales.subtotal,
    cast('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as timestamp) as actualizado_en
from {{ ref('fct_sales') }} as sales
inner join {{ ref('dim_date') }} as dates on dates.date_sk = sales.date_sk
inner join {{ ref('dim_product') }} as products on products.product_sk = sales.product_sk
inner join {{ ref('dim_sales_channel') }} as channels on channels.sales_channel_sk = sales.sales_channel_sk
left join {{ ref('dim_geography') }} as geography on geography.geography_sk = sales.geography_sk
