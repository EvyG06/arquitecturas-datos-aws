{% snapshot snap_dim_customer %}
{# unique_key: llave de negocio que identifica al cliente en la fuente. #}
{# strategy=timestamp: crea una nueva versión cuando updated_at de la fuente avanza. #}
{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    customer_id,
    name,
    email,
    phone,
    city,
    department,
    registration_date,
    updated_at
from {{ ref('int_mysql__customers__valid') }}

{% endsnapshot %}
