{# Agrega first_valid_from a cada versión SCD2: la fecha de inicio de la primera versión de la llave de negocio. #}
{% macro scd2_versions(versions_relation, business_key) %}
select
    *,
    min(valid_from) over (partition by {{ business_key }}) as first_valid_from
from {{ versions_relation }}
{% endmacro %}

{# Condición point-in-time: la versión vigente en event_date. Los eventos previos a la primera versión usan esa primera versión. #}
{% macro scd2_point_in_time_condition(version_alias, event_date) %}
(
    (
        {{ event_date }} >= {{ version_alias }}.valid_from
        and ({{ version_alias }}.valid_to is null or {{ event_date }} < {{ version_alias }}.valid_to)
    )
    or (
        {{ event_date }} < {{ version_alias }}.first_valid_from
        and {{ version_alias }}.valid_from = {{ version_alias }}.first_valid_from
    )
)
{% endmacro %}
