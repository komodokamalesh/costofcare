{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--add allowed amounts to encounters w/sline and soc/procedure data

select
ee.*,  
aa.allowed_amount::float as allowed_amount
from {{ref('ms_ocr_encounters_w_closed_flags_and_slines_and_soc')}} as ee
left join {{source('encounters', 'aa')}} as aa
on aa.procedure=ee.procedure 
and aa.encounter_key=ee.encounter_key 
and aa.service_from=ee.sl_service_from