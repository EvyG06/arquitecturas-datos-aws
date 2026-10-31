select *
from {{ ref('int_mongodb__reviews__qa') }}
where rejection_reason is not null
