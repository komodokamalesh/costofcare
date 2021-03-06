{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--add claim date, procedure, payer, setting of care and hcp/hcp info to allowed amounts pull
--note: need to review QA

with mx_encounters_w_slines as (select
e.claim_date
,e.encounter_key
,e.visit_id
,e.sources as sources_array
,e.patient_state
,e.hcp_1_npi
,e.hco_1_npi
,e.total_claim_charge_amount
,e.payer_kh_id
,e.payer_kh_id_impute_flag
,e.bill_type_code
,e.payer_channel
,e.statement_from
,e.statement_to
,e.discharge_status_code
,e.claim_type_code
,e.admit_type_code
,s.value:line_charge as line_charge
,s.value:ndc as ndc
,s.value:date_of_service as date_of_service
,s.value:service_from as sl_service_from	
,s.value:service_to	as sl_service_to
,s.value:procedure as procedure
,s.value:modifier_1	as modifier_1
,s.value:modifier_2	as modifier_2
,s.value:modifier_3	as modifier_3
,s.value:modifier_4	as modifier_4
,s.value:units as units
,s.value:place_of_service as place_of_service
,s.value:diagnosis_code_1 as diagnosis_code_1
,s.value:diagnosis_code_2 as diagnosis_code_2
,s.value:diagnosis_code_3 as diagnosis_code_3
,s.value:diagnosis_code_4 as diagnosis_code_4
,s.value:revenue_code as revenue_code
,s.value:emergency_indicator as emergency_indicator
,s.value:hco_s_1_npi as hco_s_1_npi
,s.value:hcp_s_1_npi as hcp_s_1_npi	
,s.value:hcp_s_2_npi as hcp_s_2_npi		
,s.value:service_line_number as service_line_number 
,array_contains('rowley'::variant, sources_array) as source_rowley
,array_contains('fleming-salk'::variant, sources_array) as source_fsalk
,array_contains('fleming-peso'::variant, sources_array) as source_fpeso
,array_contains('fleming-nightingale'::variant, sources_array) as source_fnight
from (select * from (select claim_date, service_lines,encounter_key, visit_id,sources,patient_state,hcp_1_npi
	,hco_1_npi,total_claim_charge_amount,payer_kh_id,payer_kh_id_impute_flag
	,bill_type_code,payer_channel,statement_from,statement_to,discharge_status_code
	,claim_type_code,admit_type_code from {{source('encounters', 'mx')}} 
	where claim_date between {{var('analysis_start_date')}} and {{var('analysis_end_date')}})
	{% if var('limit_level') > 0 %}
	{% set sample_size = 10**(var('limit_level')*2) %}
	sample ({{sample_size}} rows)
	{% endif %}
	) e,
lateral flatten(input=>e.service_lines) as s),


enhanced_encounters_with_slines as (select mx.*
,v.code_description as procedure_description
,vi.visit_setting_of_care
from mx_encounters_w_slines as mx
left join {{source('vocabulary', 'procedure')}} as v
on v.code=mx.procedure
left join (select visit_id, visit_setting_of_care from 
	{{source('encounters', 'visits')}} where year(visit_start_date)=2020) as vi
on vi.visit_id=mx.visit_id)

select
ee.*,  
aa.allowed_amount::float as allowed_amount
from enhanced_encounters_with_slines as ee
left join {{source('encounters', 'aa')}} as aa
on aa.procedure=ee.procedure and aa.encounter_key=ee.encounter_key and aa.service_from=ee.sl_service_from






