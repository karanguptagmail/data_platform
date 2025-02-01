{{ config(
    materialized='table' 
) 
}}


select
 CUSTOMERID
,CUSTOMERNAME
,CONTACTNUMBER
,EMAIL
,ADDRESS
,REGION
,CREDITCARD
,CARDPROVIDER
,GOVERNMENTID
,DATEOFBIRTH
from {{ source('raw', 'customer_detail') }}