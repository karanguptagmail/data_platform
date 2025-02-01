-- models/my_model.sql --

{{ config(
    materialized='table',
    copy_grants = true,
    cluster_by=['CUSTOMERID']
) }}

select
CUSTOMERID,
CUSTOMERNAME,
CONTACTNUMBER,
EMAIL,
ADDRESS,
CUSTOMER_REGION
from {{ source('raw', 'customer') }}