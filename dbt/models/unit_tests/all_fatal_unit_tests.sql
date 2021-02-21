{{
  config(
    materialized='table', 
  	)
}}

--pull all fatal unit tests failures

select * from {{ref('all_unit_tests')}}
where fatal_result