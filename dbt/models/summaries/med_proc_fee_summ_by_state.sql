{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--summary of medicare fees by state

select procedure
,state
,soc_type
,avg(fee) as avg_fee
from {{ref('med_proc_fee_pivot')}}
group by procedure, state, soc_type
order by procedure, state, soc_type