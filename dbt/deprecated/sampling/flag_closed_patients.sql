{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--add closed/open flag as calculation on whether PATIENT is closed during claim

with closed_flag as (select
c.claim_date,
c.encounter_key, 
c.upk_key2,
(arrays_overlap(array_construct(month(c.claim_date)), e.months_closed_mx)) as closed_flag
from {{ref('enhanced_encounters_w_allowed_amounts')}} as c
join {{source('encounters', 'eligibility')}} as e

--from {{ref('ms_ocr_encounters')}} as c
--join {{source('encounters_new', 'eligibility')}} as e
on e.upk_key2=c.upk_key2
and year(c.claim_date)=e.year
where e.months_closed_mx is not null)

--aggregate closed flag by claim_date, encounter_key, upk_key2 combination
select
claim_date, encounter_key, upk_key2,
boolor_agg(closed_flag) as closed_flag
from closed_flag
group by claim_date, encounter_key, upk_key2