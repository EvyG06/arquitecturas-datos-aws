select {{ dbt_utils.star(from=ref('int_mongodb__reviews__qa'), except=['rejection_reason']) }}
from {{ ref('int_mongodb__reviews__qa') }}
where rejection_reason is null
