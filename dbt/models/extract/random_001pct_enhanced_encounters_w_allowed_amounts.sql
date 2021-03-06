{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--pulls random 0.001 percent of enhanced encounters

select * from {{ref('enhanced_encounters_w_allowed_amounts')}} as ee
join (select encounter_key as random_key from {{ref('analysis_sample_001pct_encounter_keys')}}) as rk 
on rk.random_key=ee.encounter_key


