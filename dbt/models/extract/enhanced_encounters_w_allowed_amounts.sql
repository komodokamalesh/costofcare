{{
  config(
    materialized='table', 
    transient=false
  	)
}}

select
ee.*,  
aa.allowed_amount::float as allowed_amount
from {{ref('enhanced_encounters_w_slines')}} as ee
left join {{source('encounters', 'aa')}} as aa
on aa.procedure=ee.procedure 
and aa.encounter_key=ee.encounter_key 
and aa.service_from=ee.sl_service_from