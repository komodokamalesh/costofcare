{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--merge of 1M procedure price fees and allowed amount, filter out +/- 1%
--NOTE: inner joins?

select ee.procedure
,patient_state
,payer_kh_id
,payer_channel
,ee.claim_type_code
,modifier_1
,modifier_2
,units
,place_of_service
,visit_setting_of_care
,allowed_amount
,m.avg_fee as mpfs_avg_fee
,iff(claim_type_code='P', 'NON_FACILITY', 'FACILITY') as facility_cat
from {{ref('random_1M_enhanced_encounters_w_allowed_amounts')}} as ee
join {{ref('med_proc_fee_summ')}} as m
on m.procedure=ee.procedure and
 iff(ee.claim_type_code='P', 'NON_FACILITY', 'FACILITY')||'_AMOUNT'=m.soc_type
join {{ref('procedure_price_aa_summary')}} as pp
on pp.procedure=ee.procedure and pp.claim_type=ee.claim_type_code
where pp.percentile_1<allowed_amount and pp.percentile_99>allowed_amount

