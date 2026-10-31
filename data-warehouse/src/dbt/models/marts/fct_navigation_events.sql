{{ config(
    materialized='incremental',
    unique_key='navigation_event_sk',
    incremental_strategy='delete+insert',
    post_hook="{{ delete_orphaned_facts('navigation_event_sk', ref('int_mongodb__navigation_events__valid'), 'event_id') }}"
) }}

with events as (
    select
        event_id,
        event_type,
        customer_id,
        product_id,
        sales_channel,
        timezone('America/Bogota', occurred_at) as occurred_local_at
    from {{ ref('int_mongodb__navigation_events__valid') }}
),
customers_versioned as (
    {{ scd2_versions(ref('dim_customer'), 'customer_id') }}
),
products_versioned as (
    {{ scd2_versions(ref('dim_product'), 'product_id') }}
),
resolved_events as (
    select
        {{ dbt_utils.generate_surrogate_key(['cast(event.event_id as varchar)']) }} as navigation_event_sk,
        customer.customer_sk,
        product.product_sk,
        date.date_sk,
        sales_channel.sales_channel_sk,
        event_type.event_type_sk,
        cast(event.occurred_local_at as date) as business_date
    from events as event
    left join customers_versioned as customer
        on customer.customer_id = event.customer_id
        and {{ scd2_point_in_time_condition('customer', 'event.occurred_local_at') }}
    left join products_versioned as product
        on product.product_id = event.product_id
        and {{ scd2_point_in_time_condition('product', 'event.occurred_local_at') }}
    left join {{ ref('dim_date') }} as date
        on date.date = cast(event.occurred_local_at as date)
    left join {{ ref('dim_sales_channel') }} as sales_channel
        on sales_channel.sales_channel = event.sales_channel
    left join {{ ref('dim_event_type') }} as event_type
        on event_type.event_type = event.event_type
)

select
    candidate.navigation_event_sk,
    candidate.customer_sk,
    candidate.product_sk,
    candidate.date_sk,
    candidate.sales_channel_sk,
    candidate.event_type_sk
from resolved_events as candidate
{{ incremental_candidate_filter('navigation_event_sk', 'business_date', ['customer_sk', 'product_sk']) }}
