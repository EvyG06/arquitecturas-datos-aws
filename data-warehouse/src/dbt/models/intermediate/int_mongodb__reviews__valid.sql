select * exclude (rejection_reason)
from {{ ref('int_mongodb__reviews__qa') }}
where rejection_reason is null
