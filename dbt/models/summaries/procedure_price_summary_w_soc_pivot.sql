{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--reshaped summary of allowed amounts summary

select * 
  from {{ref('procedure_price_summary')}}
    pivot(sum(ekey_count_pct) for soc in ('Emergency Room Visit','Ambulance Visit','Outpatient Visit',
'Non-hospital institution Visit','Laboratory Visit','Inpatient Visit','Home Visit','Pharmacy Visit'))
      as p
  order by procedure