select
    *,
    case
        when name is null then 'missing_name'
        when email is null or email not like '%@%' then 'invalid_email'
    end as rejection_reason
from {{ ref('stg_mysql__customers') }}
