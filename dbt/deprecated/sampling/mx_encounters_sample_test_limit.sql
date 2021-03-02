{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--test to see how much data to draw in working sample

select claim_date, service_lines,encounter_key, visit_id,sources,patient_state,hcp_1_npi
	,hco_1_npi,total_claim_charge_amount,payer_kh_id,payer_kh_id_impute_flag
	,bill_type_code,payer_channel,statement_from,statement_to,discharge_status_code
	,claim_type_code,admit_type_code from {{source('encounters', 'mx')}} 
	limit 259000000
