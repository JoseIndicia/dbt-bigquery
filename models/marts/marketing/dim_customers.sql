with customers as (
SELECT *
  FROM {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
SELECT *
  FROM {{ ref('stg_jaffle_shop__orders') }}
),

customer_orders as (
SELECT customer_id,
       min(order_date) as first_order_date,
       max(order_date) as most_recent_order_date,
       count(order_id) as number_of_orders
  FROM orders
 GROUP BY customer_id
),

final as (
SELECT customers.customer_id,
       customers.first_name,
       customers.last_name,
       customer_orders.first_order_date,
       customer_orders.most_recent_order_date,
       customer_orders.number_of_orders
  FROM customers 
  LEFT JOIN customer_orders ON customers.customer_id = customer_orders.customer_id
)

select * from final order by customer_id