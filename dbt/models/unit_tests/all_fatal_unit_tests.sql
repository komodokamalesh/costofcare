{{
  config(
    materialized='table', 
  	)
}}

select * from {{ref('all_unit_tests')}}
where fatal_result