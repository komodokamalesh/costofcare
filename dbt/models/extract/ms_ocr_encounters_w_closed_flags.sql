{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--fold aggregated closed flag into patient table
select
ce.*,
ifnull(c.closed_flag,'False') as closed_flag
from {{ref('ms_ocr_encounters')}} ce
left join (select claim_date, encounter_key, upk_key2, closed_flag 
  from {{ref('flag_closed_patients')}} where closed_flag) as c on 
c.claim_date=ce.claim_date and c.encounter_key=ce.encounter_key and c.upk_key2=ce.upk_key2




