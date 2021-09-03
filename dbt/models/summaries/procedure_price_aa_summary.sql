{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--summary of procedure by claim_type code allowed amounts from entire analysis sample

select procedure
,procedure_description
,claim_type_code as claim_type
,count(distinct encounter_key) as count_ekey
,count(ALLOWED_AMOUNT) as count_aa
,percentile_cont(.01) within group (order by allowed_amount) as percentile_1
,percentile_cont(.05) within group (order by allowed_amount) as percentile_5
,percentile_cont(.10) within group (order by allowed_amount) as percentile_10
,stddev(ALLOWED_AMOUNT) as stddev_aa
,min(ALLOWED_AMOUNT::float) as min_aa
,avg(ALLOWED_AMOUNT::float) as average_aa
,median(ALLOWED_AMOUNT::float) as median_aa
,percentile_cont(.9) within group (order by allowed_amount) as percentile_90
,percentile_cont(.95) within group (order by allowed_amount) as percentile_95
,percentile_cont(.99) within group (order by allowed_amount) as percentile_99
,max(ALLOWED_AMOUNT::float) as max_aa
from {{ref('enhanced_encounters_w_allowed_amounts')}} as ee
group by procedure, procedure_description, claim_type_code
