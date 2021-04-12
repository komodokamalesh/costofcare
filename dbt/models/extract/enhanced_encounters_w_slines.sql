{{
  config(
    materialized='table', 
    transient=true
  	)
}}

--add setting of care, procedure description to encounters/sline data

select mx.*
,v.code_description as procedure_description
,vi.visit_setting_of_care
from {{ref('mx_encounters_w_slines')}} as mx
left join {{source('vocabulary', 'procedure')}} as v
on v.code=mx.procedure
left join {{source('encounters', 'visits')}} as vi
on vi.visit_id=mx.visit_id

