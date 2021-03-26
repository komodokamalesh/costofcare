{{
  config(
    materialized='table', 
  	)
}}

--creates a encounter_key/random val [0,1] pair for keys in enhanced encounters with allowed amounts

with MX_ENCOUNTERS_INDEX as (select distinct encounter_key, row_number() 
over (ORDER BY (SELECT NULL)) as row_number from (select distinct encounter_key 
													from {{ref('mx_encounters_w_slines')}})),


MX_ENCOUNTERS_RANDOM_INDEX as (select seq4()+1 as row_number, uniform(0::float, 1::float, random()) 
	as random_val from table(generator(rowcount => (select count(*) 
		from {{ref('mx_encounters_w_slines')}}))))

select encounter_key, random_val
from MX_ENCOUNTERS_INDEX as i
join MX_ENCOUNTERS_RANDOM_INDEX as ri
on i.row_number=ri.row_number
