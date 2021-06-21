{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--merge of procedure price fees and allowed amount

with procedure_price_table as (select
procedure
,left(procedure_description,100) as procedure_description
,iff(visit_setting_of_care='Outpatient Visit', 'NON_FACILITY', 'FACILITY') as facility_cat
,patient_state as state
,allowed_amount
from {{ref('random_01pct_enhanced_encounters_w_allowed_amounts')}}
where allowed_amount is not null
), 


procedure_price_w_mpfs as (select 
p.procedure
,procedure_description
,facility_cat
,state
,allowed_amount
,avg_fee as us_avg_mpfs
,m.soc_type
from procedure_price_table p
left join {{ref('med_proc_fee_summ')}} m
on m.procedure=p.procedure and p.facility_cat||'_AMOUNT'=m.soc_type)

select 
m.procedure
,procedure_description
,facility_cat
,m.state
,allowed_amount
,us_avg_mpfs
,avg_fee as state_avg_mpfs
from procedure_price_w_mpfs p
left join {{ref('med_proc_fee_summ_by_state')}} m
on p.facility_cat||'_AMOUNT'=m.soc_type and m.procedure=p.procedure and m.state=p.state


