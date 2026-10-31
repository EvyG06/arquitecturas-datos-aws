select *
from {{ ref('int_mysql__order_items__qa') }}
where rejection_reason is not null
