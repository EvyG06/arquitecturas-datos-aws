{# Tabla y no vista: en Redshift, si un hecho une estas filas por llave, el planificador vuelve correlacionado
   el NOT IN de las reglas, y una subconsulta correlacionada sobre staging (que tiene row_number) no esta
   soportada. Materializado, el NOT IN se resuelve una vez y los hechos leen una tabla. #}
{{ config(materialized='table') }}

select
    *,
    case
        when customer_id is null or customer_id not in (select customer_id from {{ ref('int_mysql__customers__valid') }}) then 'orphan_customer'
        when sales_channel is null or sales_channel not in ('{{ var("valid_sales_channels") | join("', '") }}') then 'invalid_sales_channel'
        when total < 0 then 'negative_total'
    end as rejection_reason
from {{ ref('stg_mysql__orders') }}
