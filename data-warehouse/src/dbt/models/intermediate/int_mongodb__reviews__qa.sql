{# Tabla y no vista: en Redshift, si un hecho une estas filas por llave, el planificador vuelve correlacionado
   el NOT IN de las reglas, y una subconsulta correlacionada sobre staging (que tiene row_number) no esta
   soportada. Materializado, el NOT IN se resuelve una vez y los hechos leen una tabla. #}
{{ config(materialized='table') }}

select
    *,
    case
        when rating is null or rating not between 1 and 5 then 'invalid_rating'
        when reviewed_at is null then 'missing_reviewed_at'
        when customer_id is null or customer_id not in (select customer_id from {{ ref('int_mysql__customers__valid') }}) then 'orphan_customer'
        when product_id is null or product_id not in (select product_id from {{ ref('int_mysql__products__valid') }}) then 'orphan_product'
    end as rejection_reason
from {{ ref('stg_mongodb__reviews') }}
