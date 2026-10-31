select
    {{ dbt_utils.generate_surrogate_key(['country', 'department', 'city']) }} as geography_sk,
    country,
    department,
    city
from {{ ref('colombia_municipalities') }}
