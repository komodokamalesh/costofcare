{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--pull encounters data for selected encounters keys
select 
claim_date
,upk_key2
,e.encounter_key
,visit_id
,total_claim_charge_amount
,patient_amount_paid
,payer_kh_id
,bill_type_code
,payer_channel
,statement_from
,service_lines
from {{source('encounters_new', 'mx')}} as e
join {{ref('ms_ocr_encounter_keys')}} as ek
on e.encounter_key=ek.encounter_key

