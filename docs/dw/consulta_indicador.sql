-- Consulta de validacion del indicador "Ventas totales" del dashboard de QuickSight.
-- Corre contra la misma tabla que consume el dashboard (marts.ventas_dashboard) y aplica los mismos filtros que
-- su control "Canal de venta". actualizado_en es la hora de la ejecucion de dbt que construyo la tabla: si
-- coincide con la columna de la tabla "Ventas por canal" del dashboard, ambos muestran el mismo corte de datos.
-- Para validar un filtro de canal, compara la fila "canal = <valor>" con el KPI despues de elegir ese valor.
-- Desde la EC2: ./orquestacion/dw.sh loading.consultas indicador
select 'Todos los canales' as filtro, count(*) as lineas, sum(subtotal) as ventas_totales,
       max(actualizado_en) as actualizado_en
from marts.ventas_dashboard
union all
select 'canal = ' || canal, count(*), sum(subtotal), max(actualizado_en)
from marts.ventas_dashboard
group by canal
order by 1;
