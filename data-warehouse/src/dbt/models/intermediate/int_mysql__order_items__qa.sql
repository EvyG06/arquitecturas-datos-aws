select
    *,
    case
        when order_id is null or order_id not in (select order_id from {{ ref('int_mysql__orders__valid') }}) then 'orphan_order'
        when product_id is null or product_id not in (select product_id from {{ ref('int_mysql__products__valid') }}) then 'orphan_product'
        when quantity <= 0 then 'invalid_quantity'
        when abs(subtotal - unit_price * quantity) > 0.01 then 'subtotal_mismatch'
    end as rejection_reason
from {{ ref('stg_mysql__order_items') }}
