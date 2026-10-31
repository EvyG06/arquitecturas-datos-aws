select *
from {{ ref('int_mysql__customers__qa') }}
where rejection_reason is not null
