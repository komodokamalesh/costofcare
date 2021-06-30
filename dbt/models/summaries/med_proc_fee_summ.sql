{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--summary of medicare fees

select procedure
,soc_type
,avg(fee) as avg_fee
from {{ref('med_proc_fee_pivot')}}
group by procedure, soc_type
order by procedure, soc_type