select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(order_date as timestamp) as order_date,
    nullif(trim(status), '') as status,
    nullif(trim(sales_channel), '') as sales_channel,
    nullif(trim(payment_method), '') as payment_method,
    cast(total as decimal(14, 2)) as total,
    _loaded_at,
    _source
from {{ source('mysql_raw', 'mysql_orders') }}
