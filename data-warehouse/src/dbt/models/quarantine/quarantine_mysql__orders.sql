select *
from {{ ref('int_mysql__orders__qa') }}
where rejection_reason is not null
