{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--add claim date, procedure, payer, setting of care and hcp/hcp info to allowed amounts pull
--note: need to review QA

select
a.encounter_key
,a.allowed_amount::float as allowed_amount
,a.procedure
,v.code_description as procedure_description
,e.patient_state
,vi.visit_setting_of_care
from (select encounter_key
  ,allowed_amount
  ,procedure from {{source('encounters', 'aa')}}
  where service_from::date between '2020-01-01' and '2020-12-31') as a
join {{source('encounters', 'mx')}} as e
on e.encounter_key=a.encounter_key
left join {{source('vocabulary', 'procedure')}} as v
on v.code=a.procedure
left join {{source('encounters', 'visits')}}  as vi
on vi.visit_id=e.visit_id
