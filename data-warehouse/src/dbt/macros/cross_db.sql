{# Diferencias de dialecto entre DuckDB y Redshift. Cada macro tiene una version por adaptador (adapter.dispatch):
   el proyecto corre en Redshift en AWS y en DuckDB para probar la logica sin gastar computo. #}

{# Hora local de negocio de un instante guardado en UTC. #}
{% macro to_business_time(column) %}{{ return(adapter.dispatch('to_business_time')(column)) }}{% endmacro %}
{% macro default__to_business_time(column) %}timezone('{{ var("business_timezone") }}', {{ column }}){% endmacro %}
{% macro redshift__to_business_time(column) %}convert_timezone('{{ var("business_timezone") }}', {{ column }}){% endmacro %}

{# Dia de la semana ISO: lunes 1 a domingo 7. Redshift no tiene isodow: su dow va de domingo 0 a sabado 6. #}
{% macro iso_day_of_week(column) %}{{ return(adapter.dispatch('iso_day_of_week')(column)) }}{% endmacro %}
{% macro default__iso_day_of_week(column) %}extract(isodow from {{ column }}){% endmacro %}
{% macro redshift__iso_day_of_week(column) %}(case when date_part(dow, {{ column }}) = 0 then 7 else date_part(dow, {{ column }}) end){% endmacro %}

{# Booleano dentro del JSON del documento. Redshift no convierte VARCHAR a BOOLEAN con cast. #}
{% macro json_boolean(column, path) %}{{ return(adapter.dispatch('json_boolean')(column, path)) }}{% endmacro %}
{% macro default__json_boolean(column, path) %}cast(json_extract_string({{ column }}, '$.{{ path | join(".") }}') as boolean){% endmacro %}
{% macro redshift__json_boolean(column, path) -%}
case lower(json_extract_path_text({{ column }}, '{{ path | join("', '") }}', true))
    when 'true' then true
    when 'false' then false
end
{%- endmacro %}

{# a IS DISTINCT FROM b, que Redshift no soporta oficialmente. Dos nulos no son distintos; un nulo y un valor si. #}
{% macro is_distinct(a, b) %}{{ return(adapter.dispatch('is_distinct')(a, b)) }}{% endmacro %}
{% macro default__is_distinct(a, b) %}{{ a }} is distinct from {{ b }}{% endmacro %}
{% macro redshift__is_distinct(a, b) %}(coalesce({{ a }} <> {{ b }}, true) and not ({{ a }} is null and {{ b }} is null)){% endmacro %}
