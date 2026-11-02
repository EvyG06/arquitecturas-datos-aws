-- Falla si la tabla del dashboard no tiene exactamente las mismas lineas y el mismo total que fct_sales:
-- el indicador de QuickSight debe coincidir con el modelo analitico.
with dashboard as (
    select count(*) as lineas, sum(subtotal) as total from {{ ref('ventas_dashboard') }}
),
facts as (
    select count(*) as lineas, sum(subtotal) as total from {{ ref('fct_sales') }}
)
select dashboard.lineas, facts.lineas, dashboard.total, facts.total
from dashboard cross join facts
where dashboard.lineas <> facts.lineas or dashboard.total <> facts.total
