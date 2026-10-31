-- Falla si dim_date no tiene exactamente un registro por día del rango configurado en dbt_project.yml.
with bounds as (
    select
        count(*) as actual_rows,
        datediff(
            'day',
            cast('{{ var("date_dimension_start") }}' as date),
            cast('{{ var("date_dimension_end") }}' as date)
        ) as expected_rows
    from {{ ref('dim_date') }}
)
select *
from bounds
where actual_rows <> expected_rows
