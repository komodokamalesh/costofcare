{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--creates a encounter_key/random val [0,1] pair for keys in enhanced encounters with allowed amounts

with MX_ENCOUNTERS_INDEX as (select encounter_key, row_number() 
over (ORDER BY (SELECT NULL)) as row_number from {{ref('enhanced_encounters_w_allowed_amounts')}}),


MX_ENCOUNTERS_RANDOM_INDEX as (select seq4()+1 as row_number, uniform(0::float, 1::float, random()) 
	as random_val from table(generator(rowcount => (select count(*) from mx_encounters_test)))),

select encounter_key, random_val
from MX_ENCOUNTERS_TEST_INDEX as i
join MX_ENCOUNTERS_TEST_RANDOM_INDEX as ri
on i.row_number=ri.row_number
