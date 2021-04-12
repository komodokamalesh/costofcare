{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--add allowed amounts to encounters w/sline and soc/procedure data

select sum(allowed_amount) as aa_total
,upk_key2
,visit_setting_of_care as soc
from {{ref('enhanced_encounters_w_allowed_amounts')}}
group by upk_key2, visit_setting_of_care