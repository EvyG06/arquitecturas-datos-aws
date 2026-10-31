-- Falla si la fecha de una reseña o de un evento no es el día local de Bogotá del documento original.
with expected_review_dates as (
    select
        {{ dbt_utils.generate_surrogate_key(['cast(review_id as varchar)']) }} as review_sk,
        dates.date_sk
    from {{ ref('int_mongodb__reviews__valid') }} as reviews
    inner join {{ ref('dim_date') }} as dates
        on dates.date = cast(timezone('America/Bogota', reviews.reviewed_at) as date)
),
expected_event_dates as (
    select
        {{ dbt_utils.generate_surrogate_key(['cast(event_id as varchar)']) }} as navigation_event_sk,
        dates.date_sk
    from {{ ref('int_mongodb__navigation_events__valid') }} as events
    inner join {{ ref('dim_date') }} as dates
        on dates.date = cast(timezone('America/Bogota', events.occurred_at) as date)
)
select
    expected.review_sk as sk,
    expected.date_sk as expected_date_sk,
    facts.date_sk as fact_date_sk
from expected_review_dates as expected
inner join {{ ref('fct_reviews') }} as facts on facts.review_sk = expected.review_sk
where expected.date_sk is distinct from facts.date_sk

union all

select
    expected.navigation_event_sk,
    expected.date_sk,
    facts.date_sk
from expected_event_dates as expected
inner join {{ ref('fct_navigation_events') }} as facts on facts.navigation_event_sk = expected.navigation_event_sk
where expected.date_sk is distinct from facts.date_sk
