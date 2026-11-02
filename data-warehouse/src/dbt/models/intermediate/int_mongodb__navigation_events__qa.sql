{# Tabla y no vista: en Redshift, si un hecho une estas filas por llave, el planificador vuelve correlacionado
   el NOT IN de las reglas, y una subconsulta correlacionada sobre staging (que tiene row_number) no esta
   soportada. Materializado, el NOT IN se resuelve una vez y los hechos leen una tabla. #}
{{ config(materialized='table') }}

select
    *,
    case
        when event_type is null or event_type not in ('{{ var("valid_event_types") | join("', '") }}') then 'invalid_event_type'
        when occurred_at is null then 'missing_occurred_at'
        when product_id is null or product_id not in (select product_id from {{ ref('int_mysql__products__valid') }}) then 'orphan_product'
        when customer_id is not null and customer_id not in (select customer_id from {{ ref('int_mysql__customers__valid') }}) then 'orphan_customer'
        when sales_channel is null or sales_channel not in ('{{ var("valid_sales_channels") | join("', '") }}') then 'invalid_sales_channel'
    end as rejection_reason
from {{ ref('stg_mongodb__navigation_events') }}
