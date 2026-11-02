{# Ultima version de cada llave en raw. En AWS raw es de solo insercion: cada lote trae solo lo que cambio,
   y el solape de la ventana CDC puede traer la misma fila en dos lotes. Gana la marca de cambio mas reciente
   de la fuente (_cdc_ts); si empatan, la extraccion mas reciente. Asi repetir un lote nunca duplica filas. #}
{% macro latest_version(relation, key) %}
select *
from (
    select
        *,
        row_number() over (
            partition by {{ key }}
            order by _cdc_ts desc nulls last, _extracted_at desc
        ) as _version_rank
    from {{ relation }}
) as versions
where _version_rank = 1
{% endmacro %}
