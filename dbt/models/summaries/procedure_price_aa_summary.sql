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
,approx_percentile(ALLOWED_AMOUNT::float,.01) as percentile_1
,approx_percentile(ALLOWED_AMOUNT::float,.05) as percentile_5
,approx_percentile(ALLOWED_AMOUNT::float,.1) as percentile_10
,stddev(ALLOWED_AMOUNT) as stddev_aa
,min(ALLOWED_AMOUNT::float) as min_aa
,avg(ALLOWED_AMOUNT::float) as average_aa
,median(ALLOWED_AMOUNT::float) as median_aa
,approx_percentile(ALLOWED_AMOUNT::float,.90) as percentile_90
,approx_percentile(ALLOWED_AMOUNT::float,.95) as percentile_95
,approx_percentile(ALLOWED_AMOUNT::float,.99) as percentile_99
,max(ALLOWED_AMOUNT::float) as max_aa
from {{ref('enhanced_encounters_w_allowed_amounts')}} as ee
group by procedure, procedure_description, claim_type_code
