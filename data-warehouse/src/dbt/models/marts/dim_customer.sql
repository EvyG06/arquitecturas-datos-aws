select
    {{ dbt_utils.generate_surrogate_key(["'customer'", 'customer_id', 'dbt_valid_from']) }} as customer_sk,
    customer_id,
    name,
    email,
    phone,
    city,
    department,
    registration_date,
    updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_dim_customer') }}
