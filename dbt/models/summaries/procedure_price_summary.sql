{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--summary of procedure allowed amounts from analysis sample

with procedure_amt_summ as (select
procedure 
,count(distinct encounter_key) as proc_ekey_count
from {{ref('random_01pct_enhanced_encounters_w_allowed_amounts')}}
group by procedure
),

procedure_stats_summ as (select procedure
,left(procedure_description,100) as procedure_description
,visit_setting_of_care as soc
,count(distinct encounter_key) as count_ekey
,count(line_charge) as count_lc
,count(total_claim_charge_amount) as count_tcca
,count(ALLOWED_AMOUNT) as count_aa
,stddev(ALLOWED_AMOUNT) as stddev_aa
,min(ALLOWED_AMOUNT::float) as min_aa
,avg(ALLOWED_AMOUNT::float) as average_aa
,median(ALLOWED_AMOUNT::float) as median_aa
,max(ALLOWED_AMOUNT::float) as max_aa
from {{ref('random_01pct_enhanced_encounters_w_allowed_amounts')}} as ee
group by procedure, left(procedure_description, 100), visit_setting_of_care)

select 
ssumm.procedure
,procedure_description
,soc
,count_ekey
,proc_ekey_count
,count_ekey/proc_ekey_count as ekey_count_pct
,count_lc
,count_tcca
,count_aa
,stddev_aa
,min_aa
,average_aa
,median_aa
,max_aa
from procedure_stats_summ as ssumm
join procedure_amt_summ as asumm
on ssumm.procedure=asumm.procedure
order by proc_ekey_count desc, count_ekey desc