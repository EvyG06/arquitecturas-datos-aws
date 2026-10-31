select
    {{ dbt_utils.generate_surrogate_key(["'product'", 'product_id', 'dbt_valid_from']) }} as product_sk,
    product_id,
    name,
    category,
    price,
    description,
    is_active,
    updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_dim_product') }}
