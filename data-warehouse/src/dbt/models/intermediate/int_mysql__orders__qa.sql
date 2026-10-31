select
    *,
    case
        when customer_id is null or customer_id not in (select customer_id from {{ ref('int_mysql__customers__valid') }}) then 'orphan_customer'
        when sales_channel is null or sales_channel not in ('{{ var("valid_sales_channels") | join("', '") }}') then 'invalid_sales_channel'
        when total < 0 then 'negative_total'
    end as rejection_reason
from {{ ref('stg_mysql__orders') }}
