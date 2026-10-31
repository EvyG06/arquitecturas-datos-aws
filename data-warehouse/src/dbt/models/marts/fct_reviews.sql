{{ config(
    materialized='incremental',
    unique_key='review_sk',
    incremental_strategy='delete+insert',
    post_hook="{{ delete_orphaned_facts('review_sk', ref('int_mongodb__reviews__valid'), 'review_id') }}"
) }}

with reviews as (
    select
        review_id,
        customer_id,
        product_id,
        rating,
        is_verified_purchase,
        timezone('America/Bogota', reviewed_at) as reviewed_local_at
    from {{ ref('int_mongodb__reviews__valid') }}
),
customers_versioned as (
    {{ scd2_versions(ref('dim_customer'), 'customer_id') }}
),
products_versioned as (
    {{ scd2_versions(ref('dim_product'), 'product_id') }}
),
resolved_reviews as (
    select
        {{ dbt_utils.generate_surrogate_key(['cast(review.review_id as varchar)']) }} as review_sk,
        customer.customer_sk,
        product.product_sk,
        date.date_sk,
        review.rating,
        review.is_verified_purchase,
        cast(review.reviewed_local_at as date) as business_date
    from reviews as review
    left join customers_versioned as customer
        on customer.customer_id = review.customer_id
        and {{ scd2_point_in_time_condition('customer', 'review.reviewed_local_at') }}
    left join products_versioned as product
        on product.product_id = review.product_id
        and {{ scd2_point_in_time_condition('product', 'review.reviewed_local_at') }}
    left join {{ ref('dim_date') }} as date
        on date.date = cast(review.reviewed_local_at as date)
)

select
    candidate.review_sk,
    candidate.customer_sk,
    candidate.product_sk,
    candidate.date_sk,
    candidate.rating,
    candidate.is_verified_purchase
from resolved_reviews as candidate
{{ incremental_candidate_filter('review_sk', 'business_date', ['customer_sk', 'product_sk']) }}
