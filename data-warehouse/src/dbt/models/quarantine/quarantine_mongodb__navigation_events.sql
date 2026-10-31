select *
from {{ ref('int_mongodb__navigation_events__qa') }}
where rejection_reason is not null
