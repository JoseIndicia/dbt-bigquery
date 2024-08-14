with customers as (
SELECT *
  FROM {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
SELECT *
  FROM {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
SELECT *
  FROM {{ ref('stg_stripe__payments') }}
),

order_payments as (
SELECT customers.customer_id,
       orders.order_id,
       sum(case when payments.payment_status = 'success' then payments.amount end) as amount
  FROM orders 
  LEFT JOIN payments ON orders.order_id = payments.order_id
  LEFT JOIN customers ON orders.customer_id = customers.customer_id
 GROUP BY customers.customer_id,
          orders.order_id
)

select customer_id,
       order_id,
       COALESCE(amount,0) as amount
  from order_payments