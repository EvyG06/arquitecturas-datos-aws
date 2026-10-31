{% set start_date = var('date_dimension_start') %}
{% set end_date = var('date_dimension_end') %}

with date_spine as (
    select cast(date_day as date) as date
    from (
        {{ dbt_utils.date_spine(
        datepart='day',
        start_date="cast('" ~ start_date ~ "' as date)",
        end_date="cast('" ~ end_date ~ "' as date)"
        ) }}
    ) as spine
),
holidays as (
    select cast(date as date) as date, holiday_name
    from {{ ref('colombia_holidays') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['cast(date as date)']) }} as date_sk,
    cast(date as date) as date,
    cast(extract(year from date) as integer) as year,
    cast(extract(quarter from date) as integer) as quarter,
    cast(extract(month from date) as integer) as month,
    cast(extract(week from date) as integer) as week,
    cast(extract(isodow from date) as integer) as day_of_week_number,
    case extract(isodow from date)
        when 1 then 'lunes'
        when 2 then 'martes'
        when 3 then 'miércoles'
        when 4 then 'jueves'
        when 5 then 'viernes'
        when 6 then 'sábado'
        when 7 then 'domingo'
    end as day_of_week,
    cast(extract(day from date) as integer) as day_of_month,
    extract(isodow from date) in (6, 7) as is_weekend,
    holidays.holiday_name is not null as is_holiday,
    holidays.holiday_name as holiday_name
from date_spine
left join holidays using (date)
