      
{{
  config(
    materialized='table', 
    transient=false
  	)
}}

with count_table as (select encounter_key
,payer_channel
, upk_key2
, count(encounter_key) as n_ek
, count(allowed_amount) as n_aa
, count(line_charge) as n_lc
, count(patient_amount_paid) as n_pap
, count(total_claim_charge_amount)  as n_tcca
from {{ref('enhanced_encounters_w_allowed_amounts_mso_patients')}}
group by encounter_key, payer_channel, upk_key2)


select count(distinct encounter_key) as encounter_key_count
,count(distinct upk_key2) as patient_key_count
, payer_channel
,count_if(n_aa>0)/encounter_key_count as allowed_amount_fill_rate
,count_if(n_lc>0)/encounter_key_count as line_charge_fill_rate
,count_if(n_pap>0)/encounter_key_count as patient_amount_paid_fill_rate
,count_if(n_tcca>0)/encounter_key_count as total_claim_charge_amount_fill_rate
from count_table group by payer_channel
order by count(encounter_key) desc