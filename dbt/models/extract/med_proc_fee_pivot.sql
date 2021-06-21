{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--table to summarize medicare fees by procedure, state, facility/non-facility 

with locality_state_map as (select
locality, state 
from {{source('med_fee', 'locality')}} 
group by locality, state),

med_price as (select
procedure
,non_facility_amount
,facility_amount
,state
from {{source('med_fee', 'mpfs')}} as mpfs
join locality_state_map as lsm
on mpfs.locality=lsm.locality
)


select * from med_price
unpivot(fee for soc_type in (non_facility_amount, facility_amount))
order by procedure, state, soc_type

