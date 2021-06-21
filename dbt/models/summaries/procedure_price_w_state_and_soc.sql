{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--merge of state-level medicare fee data and allowed amounts

with procedure_price_table as (select procedure
,left(procedure_description,100) as procedure_description
,visit_setting_of_care as soc
,patient_state as state
,ALLOWED_AMOUNT as aa
from {{ref('random_01pct_enhanced_encounters_w_allowed_amounts')}} as ee
where allowed_amount is not null),


locality_state_map as (select
locality, state 
from {{source('med_fee', 'locality')}} 
group by locality, state),

med_price as (select
'"'||procedure||'"' as proc 
,non_facility_amount
,facility_amount
,state
from {{source('med_fee', 'mpfs')}} as mpfs
join locality_state_map as lsm
on mpfs.locality=lsm.locality
limit 100
)


select 
* from med_price limit 10000