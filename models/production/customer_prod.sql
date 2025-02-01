-- models/my_model.sql --

{{ config(
    materialized='table_with_inline_masking',
    masking_policy = {
        'EMAIL': 'pii_policy_tag_prod'
    } 
) }}

select
CUSTOMERID,
CUSTOMERNAME,
CONTACTNUMBER,
EMAIL,
ADDRESS,
CUSTOMER_REGION
from {{ source('raw', 'customer') }}