-- Ventas por mes. Cada fila del hecho es una línea de pedido.
select
    calendar.year,
    calendar.month,
    sum(sales.subtotal) as total_sales,
    sum(sales.quantity) as units_sold,
    count(*) as order_lines
from marts.fct_sales as sales
join marts.dim_date as calendar
  on sales.date_sk = calendar.date_sk
group by calendar.year, calendar.month
order by calendar.year, calendar.month;

-- Ventas por departamento, usando la llave de geografía del hecho.
select
    geography.department,
    sum(sales.subtotal) as total_sales,
    count(*) as order_lines
from marts.fct_sales as sales
join marts.dim_geography as geography
  on sales.geography_sk = geography.geography_sk
group by geography.department
order by total_sales desc;

-- Historia SCD2 del cliente 1: la llave subrogada cambia por cada versión.
select
    customer.customer_id as business_key,
    customer.customer_sk as version_surrogate_key,
    customer.city,
    customer.department,
    customer.valid_from,
    customer.valid_to,
    customer.is_current
from marts.dim_customer as customer
where customer.customer_id = 1
order by customer.valid_from;

-- Drill-across: ventas, reseñas y visitas por producto y mes. Cada hecho se agrega
-- por separado antes de unirlo, para no multiplicar filas entre hechos de distinto grano.
with sales_by_month as (
    select
        product.product_id,
        calendar.year,
        calendar.month,
        sum(sales.subtotal) as total_sales
    from marts.fct_sales as sales
    join marts.dim_product as product
      on sales.product_sk = product.product_sk
    join marts.dim_date as calendar
      on sales.date_sk = calendar.date_sk
    group by product.product_id, calendar.year, calendar.month
),
reviews_by_month as (
    select
        product.product_id,
        calendar.year,
        calendar.month,
        count(*) as review_count,
        avg(reviews.rating) as average_rating
    from marts.fct_reviews as reviews
    join marts.dim_product as product
      on reviews.product_sk = product.product_sk
    join marts.dim_date as calendar
      on reviews.date_sk = calendar.date_sk
    group by product.product_id, calendar.year, calendar.month
),
events_by_month as (
    select
        product.product_id,
        calendar.year,
        calendar.month,
        count(*) as navigation_events
    from marts.fct_navigation_events as events
    join marts.dim_product as product
      on events.product_sk = product.product_sk
    join marts.dim_date as calendar
      on events.date_sk = calendar.date_sk
    group by product.product_id, calendar.year, calendar.month
)
select
    coalesce(sales.product_id, reviews.product_id, events.product_id) as product_id,
    coalesce(sales.year, reviews.year, events.year) as year,
    coalesce(sales.month, reviews.month, events.month) as month,
    sales.total_sales,
    reviews.review_count,
    reviews.average_rating,
    events.navigation_events
from sales_by_month as sales
full outer join reviews_by_month as reviews
    on sales.product_id = reviews.product_id
    and sales.year = reviews.year
    and sales.month = reviews.month
full outer join events_by_month as events
    on coalesce(sales.product_id, reviews.product_id) = events.product_id
    and coalesce(sales.year, reviews.year) = events.year
    and coalesce(sales.month, reviews.month) = events.month
order by year, month, product_id;
