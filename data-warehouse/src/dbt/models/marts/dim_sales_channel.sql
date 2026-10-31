-- SCD tipo 1: refleja en cada ejecución los canales presentes en órdenes y eventos válidos.
with channels as (
    select sales_channel from {{ ref('int_mysql__orders__valid') }}
    union
    select sales_channel from {{ ref('int_mongodb__navigation_events__valid') }}
),

distinct_channels as (
    select distinct nullif(trim(sales_channel), '') as sales_channel
    from channels
)

select
    {{ dbt_utils.generate_surrogate_key(['sales_channel']) }} as sales_channel_sk,
    sales_channel
from distinct_channels
where sales_channel is not null
