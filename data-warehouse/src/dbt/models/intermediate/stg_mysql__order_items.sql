select
    cast(order_item_id as bigint) as order_item_id,
    cast(order_id as bigint) as order_id,
    cast(product_id as bigint) as product_id,
    cast(quantity as integer) as quantity,
    cast(unit_price as decimal(12, 2)) as unit_price,
    cast(subtotal as decimal(14, 2)) as subtotal,
    _batch_id,
    _cdc_ts,
    _loaded_at,
    _source
from ({{ latest_version(source('mysql_raw', 'mysql_order_items'), 'order_item_id') }}) as latest
