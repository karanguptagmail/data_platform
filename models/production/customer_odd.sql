{{ 
  config(
    materialized = 'table',
    database = 'dagster_load',
    schema = 'raw',
    copy_grants = true
) 
}}

select 
customer_id,
interest_earned
from {{ ref('customer_account_balance') }}
where true
and mod(customer_id, 2) != 0