select
    *,
    case
        when category is null then 'missing_category'
        when price < 0 then 'negative_price'
    end as rejection_reason
from {{ ref('stg_mysql__products') }}
