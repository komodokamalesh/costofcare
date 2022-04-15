{{
  config(
    materialized='table', 
    transient=false
  	)
}}



select 
e.claim_date
,upk_key2
,e.encounter_key
,e.closed_flag
,visit_id
,total_claim_charge_amount
,patient_amount_paid
,payer_kh_id
,bill_type_code
,payer_channel
,statement_from
,s.value:line_charge as line_charge
,s.value:date_of_service as date_of_service
,s.value:service_from as sl_service_from  
,s.value:service_to as sl_service_to
,s.value:procedure as procedure
,s.value:ndc as ndc
from {{ref('ms_ocr_encounters_w_closed_flags')}} as e, 
lateral flatten(input=>e.service_lines) as s


