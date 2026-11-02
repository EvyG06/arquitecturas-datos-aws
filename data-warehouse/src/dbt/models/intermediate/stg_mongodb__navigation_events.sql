select
    cast(event_id as bigint) as event_id,
    nullif(trim(event_type), '') as event_type,
    cast(occurred_at as timestamptz) as occurred_at,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    nullif(trim(sales_channel), '') as sales_channel,
    document_json,
    _batch_id,
    _cdc_ts,
    _loaded_at,
    _source
from ({{ latest_version(source('mongodb_raw', 'mongo_navigation_events'), 'event_id') }}) as latest
