{% snapshot snap_dim_product %}
{# unique_key: llave de negocio que identifica al producto en la fuente. #}
{# strategy=timestamp: crea una nueva versión cuando updated_at de la fuente avanza. #}
{{
    config(
        unique_key='product_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    product_id,
    name,
    category,
    price,
    description,
    is_active,
    updated_at
from {{ ref('int_mysql__products__valid') }}

{% endsnapshot %}
