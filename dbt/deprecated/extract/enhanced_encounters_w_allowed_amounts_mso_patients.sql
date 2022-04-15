{{
  config(
    materialized='table', 
    transient=true
  	)
}}


select e.* from {{ref('enhanced_encounters_w_allowed_amounts')}} e
join {{ref('ms_ocre_patients')}} ms
on ms.upk_key2=e.upk_key2