{{ 
  config(
    materialized = 'incremental',
    unique_key = ['customer_sk'],
    incremental_strategy = 'merge',
    tags = ['manual/incremental_run'],
    copy_grants = true
) 
}}

with cte as (
select 
c_customer_sk,
c_customer_id,
c_current_cdemo_sk,
c_current_hdemo_sk,
c_current_addr_sk,
c_salutation,
c_first_name,
c_last_name,
c_birth_country,
c_email_address,
load_dt,
update_dt
from {{ source('raw', 'customer_data') }} c
{% if is_incremental() %}
  where coalesce(c.update_dt, c.load_dt) >= to_date('{{ var("min_date") }}')
  and coalesce(c.update_dt, c.load_dt) < to_date('{{ var("min_date") }}') + interval '1 day'
{% endif %}
)

select 
c_customer_sk as customer_sk,
c_customer_id as customer_id,
c_salutation as customer_salutation,
c_first_name as first_name,
c_last_name as last_name,
c_birth_country as birth_country,
c_email_address as email_address,
c.load_dt as customer_load_dt,
c.update_dt as customer_update_dt
from cte c
where true

-- and coalesce(c.update_dt, c.load_dt) > ( select coalesce(max(coalesce(customer_update_dt, customer_load_dt)), '1900-01-01') from {{ this }} )
-- and coalesce(c.update_dt, c.load_dt) >= ( select coalesce(max(coalesce(address_update_dt, address_load_dt)), '1900-01-01') - interval '2 days' from {{ this }} )