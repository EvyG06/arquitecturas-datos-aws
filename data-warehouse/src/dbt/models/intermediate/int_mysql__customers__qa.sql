{# Tabla y no vista: en Redshift, si un hecho une estas filas por llave, el planificador vuelve correlacionado
   el NOT IN de las reglas, y una subconsulta correlacionada sobre staging (que tiene row_number) no esta
   soportada. Materializado, el NOT IN se resuelve una vez y los hechos leen una tabla. #}
{{ config(materialized='table') }}

select
    *,
    case
        when name is null then 'missing_name'
        when email is null or email not like '%@%' then 'invalid_email'
    end as rejection_reason
from {{ ref('stg_mysql__customers') }}
