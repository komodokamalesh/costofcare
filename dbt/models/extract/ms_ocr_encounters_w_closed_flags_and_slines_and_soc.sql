{{
  config(
    materialized='table', 
    transient=true
  	)
}}

--add setting of care to encounters/sline data

select e.*
,vi.visit_setting_of_care
from {{ref('ms_ocr_encounters_w_closed_flags_and_slines')}} as e
left join {{source('encounters', 'visits')}} as vi
on vi.visit_id=e.visit_id

