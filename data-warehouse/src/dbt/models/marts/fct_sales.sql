{{ config(
    materialized='incremental',
    unique_key='sale_sk',
    incremental_strategy='delete+insert',
    post_hook="{{ delete_orphaned_facts('sale_sk', ref('int_mysql__order_items__valid'), 'order_item_id') }}"
) }}

with line_items as (
    select
        line_item.order_item_id,
        line_item.product_id,
        order_header.customer_id,
        order_header.sales_channel,
        order_header.order_date,
        cast(order_header.order_date as date) as date,
        line_item.quantity,
        line_item.unit_price,
        line_item.subtotal
    from {{ ref('int_mysql__order_items__valid') }} as line_item
    inner join {{ ref('int_mysql__orders__valid') }} as order_header using (order_id)
),
customers_versioned as (
    {{ scd2_versions(ref('dim_customer'), 'customer_id') }}
),
products_versioned as (
    {{ scd2_versions(ref('dim_product'), 'product_id') }}
),
resolved_sales as (
    select
        {{ dbt_utils.generate_surrogate_key(['cast(line_items.order_item_id as varchar)']) }} as sale_sk,
        customer.customer_sk,
        product.product_sk,
        sales_channel.sales_channel_sk,
        date.date_sk,
        geography.geography_sk,
        line_items.quantity,
        line_items.unit_price,
        line_items.subtotal,
        line_items.date as business_date
    from line_items
    left join customers_versioned as customer
        on customer.customer_id = line_items.customer_id
        and {{ scd2_point_in_time_condition('customer', 'line_items.order_date') }}
    left join products_versioned as product
        on product.product_id = line_items.product_id
        and {{ scd2_point_in_time_condition('product', 'line_items.order_date') }}
    left join {{ ref('dim_sales_channel') }} as sales_channel
        on sales_channel.sales_channel = line_items.sales_channel
    left join {{ ref('dim_date') }} as date
        on date.date = line_items.date
    left join {{ ref('dim_geography') }} as geography
        on geography.country = 'Colombia'
        and geography.department = customer.department
        and geography.city = customer.city
)

select
    candidate.sale_sk,
    candidate.customer_sk,
    candidate.product_sk,
    candidate.sales_channel_sk,
    candidate.date_sk,
    candidate.geography_sk,
    candidate.quantity,
    candidate.unit_price,
    candidate.subtotal
from resolved_sales as candidate
{{ incremental_candidate_filter('sale_sk', 'business_date', ['customer_sk', 'product_sk', 'geography_sk']) }}
