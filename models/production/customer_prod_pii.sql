-- models/my_model.sql --
-- working --
-- cluster by --
-- dynamic_tag_policy --
-- copy grants --
-- table type {creates temporary, transient and permanent as per the configuration} --
-- pre_hook --
-- post_hook --

{{ config(
    materialized='table',
    transient = false,
    pre_hook = "delete from genai.raw.customer_detail where true and customerid = '1'",
    post_hook = "delete from genai.raw.customer_detail where true and customerid = '19'",
    cluster_by=['CUSTOMERID'], 
    copy_grants = true,
    dynamic_tag_policy = {
        'EMAIL': 'pii_policy_tag_prod',
        'CONTACTNUMBER': 'pii_policy_tag_prod'
    } 
) }}

with cte as (
select
CUSTOMERID,
CUSTOMERNAME,
CONTACTNUMBER,
EMAIL,
ADDRESS,
CUSTOMER_REGION,
row_number()over(order by customerid) as rn
from {{ source('raw', 'customer') }}
)
-- wrap transformation in cte --
-- final select should end with from --
select
CUSTOMERID,
CUSTOMERNAME,
CONTACTNUMBER,
EMAIL,
ADDRESS,
CUSTOMER_REGION,
rn
from cte

-- https://up88851.us-east-2.aws.snowflakecomputing.com --