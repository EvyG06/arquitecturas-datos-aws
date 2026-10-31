select *
from {{ ref('int_mysql__products__qa') }}
where rejection_reason is not null
