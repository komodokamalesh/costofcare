{{
  config(
    materialized='table', 
  	)
}}

--pull all unit test failures

select * from {{ref('all_unit_tests')}}
where not test_passed