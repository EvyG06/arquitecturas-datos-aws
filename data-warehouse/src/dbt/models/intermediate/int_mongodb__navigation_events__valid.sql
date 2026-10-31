select * exclude (rejection_reason)
from {{ ref('int_mongodb__navigation_events__qa') }}
where rejection_reason is null
