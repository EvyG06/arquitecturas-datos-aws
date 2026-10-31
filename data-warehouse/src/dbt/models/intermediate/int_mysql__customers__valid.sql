select * exclude (rejection_reason)
from {{ ref('int_mysql__customers__qa') }}
where rejection_reason is null
