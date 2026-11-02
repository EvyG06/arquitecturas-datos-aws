select {{ dbt_utils.star(from=ref('int_mysql__orders__qa'), except=['rejection_reason']) }}
from {{ ref('int_mysql__orders__qa') }}
where rejection_reason is null
