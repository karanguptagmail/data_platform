{{ 
  config(
    materialized = 'incremental'
) 
}}

select '{{ is_incremental() }}' as is_incremental_check
from {{ ref('customer_detail_prod') }}

-- the incremental model {{ this }} should itself be present --
-- first run is always incremental false --
-- second run onwards is incremental true --
