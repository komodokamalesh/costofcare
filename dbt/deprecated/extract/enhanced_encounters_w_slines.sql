{{
  config(
    materialized='table', 
    transient=false
  	)
}}


select mx.*
,v.code_description as procedure_description
,vi.visit_setting_of_care
from {{ref('mx_encounters_w_slines')}} as mx
left join {{source('vocabulary', 'procedure')}} as v
on v.code=mx.procedure
left join (select visit_id, visit_setting_of_care from 
	{{source('encounters', 'visits')}} where year(visit_start_date)=2020) as vi
on vi.visit_id=mx.visit_id

