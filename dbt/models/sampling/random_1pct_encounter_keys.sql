{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--pulls random 1 percent of encounter keys

select encounter_key
from {{ref('encounter_keys_w_random_vals')}} as ri
where ri.random_val<=.01


