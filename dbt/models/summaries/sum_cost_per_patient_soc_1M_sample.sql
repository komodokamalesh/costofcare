{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--add allowed amounts to encounters w/sline and soc/procedure data

select *
from {{ref('sum_cost_per_patient_soc')}} sample (1000000 rows)
