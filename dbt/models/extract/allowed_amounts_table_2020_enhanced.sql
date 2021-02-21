{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--add claim date, procedure, payer, setting of care and hcp/hcp info to allowed amounts pull
--note: need to review QA

select
e.claim_date
,a.encounter_key
,a.allowed_amount
,a.procedure
,a.sources_array
,v.code_description as procedure_description
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
,vi.visit_setting_of_care
,array_contains('rowley'::variant, a.sources_array) as source_rowley
,array_contains('fleming-salk'::variant, a.sources_array) as source_fsalk
,array_contains('fleming-peso'::variant, a.sources_array) as source_fpeso
,array_contains('fleming-nightingale'::variant, a.sources_array) as source_fnight
from {{ref('allowed_amounts_table_2020')}} as a
join (select visit_id, encounter_key, claim_date, patient_state, hcp_1_npi, hco_1_npi, total_claim_charge_amount, 
      payer_kh_id, payer_kh_id_impute_flag,bill_type_code,payer_channel,statement_from,
        statement_to,discharge_status_code,claim_type_code,admit_type_code from
        {{source('encounters', 'mx')}} where year(claim_date)=2020) as e
on e.encounter_key=a.encounter_key
left join {{source('vocabulary', 'procedure')}} as v
on v.code=a.procedure
left join (select visit_id, visit_setting_of_care from 
	{{source('encounters', 'visits')}} where year(visit_start_date)=2020) as vi
on vi.visit_id=e.visit_id
