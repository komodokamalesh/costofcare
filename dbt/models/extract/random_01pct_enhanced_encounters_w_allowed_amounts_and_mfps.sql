{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--merge of  medicare procedure price fees and 1% of sampled encounters/allowed amount

select ee.*
,m.avg_fee as mpfs_avg_fee
,iff(claim_type_code='P', 'NON_FACILITY', 'FACILITY') as facility_cat
from {{ref('random_01pct_enhanced_encounters_w_allowed_amounts')}} as ee
left join {{ref('med_proc_fee_summ')}} m
on m.procedure=p.procedure and ee.facility_cat||'_AMOUNT'=m.soc_type


