{{ 
  config(
    materialized = 'table',
    copy_grants = true
) 
}}

with recursive cte as (
select min(transaction_date) as transaction_date
from {{ ref('transaction_data') }}
union all
select transaction_date + interval '1 day' as transaction_date
from cte
where transaction_date < (select max(transaction_date) from {{ ref('transaction_data') }})
),
cte1 as (
select c1.customer_id, transaction_date
from cte c
cross join
(
select distinct customer_id from {{ ref('transaction_data') }}
) c1
),
cte2 as (
select distinct customer_id, transaction_date, sum(amount)over(partition by customer_id order by transaction_date) as running_total
from {{ ref('transaction_data') }}
),
cte3 as (
select customer_id, transaction_date, running_total,
lead(transaction_date, 1, '2024-04-01')over(partition by customer_id order by transaction_date) as next_transaction_date,
lead(transaction_date, 1, '2024-04-01')over(partition by customer_id order by transaction_date) - transaction_date as difference_days
from cte2
)
select customer_id,
sum(running_total * difference_days * interest_rate) as interest_earned 
from cte3 c3
join {{ ref('interest_rates') }} r
on
c3.running_total between r.min_balance and r.max_balance
group by customer_id
union all
select customer_id,
amount
from {{ source('raw', 'upstream_python_asset') }}