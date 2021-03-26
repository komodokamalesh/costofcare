{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--pulls random 1M encounter keys

select encounter_key
from {{ref('analysis_sample_encounter_keys_w_random_vals')}} as ri
order by random_val
limit 10000000
--where ri.random_val<=.01;


