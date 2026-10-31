-- Falla si algún año de dim_date no tiene festivos cargados en el seed colombia_holidays.
with years as (
    select distinct year from {{ ref('dim_date') }}
),
years_with_holidays as (
    select distinct year from {{ ref('dim_date') }} where is_holiday
)
select years.year
from years
left join years_with_holidays using (year)
where years_with_holidays.year is null
