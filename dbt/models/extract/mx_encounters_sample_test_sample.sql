{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--test to see how much data to draw in working sample

select seq4() as row_num, uniform(0::float, 1::float, random()) as rand_num
from table(generator(rowcount => (select count(*) 
	from {{source('encounters', 'mx')}}))) 
where rand_num<.0001