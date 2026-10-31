-- SCD tipo 1: refleja en cada ejecución los tipos de evento válidos presentes en la fuente.
with event_types as (
    select distinct event_type
    from {{ ref('int_mongodb__navigation_events__valid') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_type']) }} as event_type_sk,
    event_type
from event_types
