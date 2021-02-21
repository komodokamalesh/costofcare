{{
  config(
    materialized='table', 
  	)
}}

-- unit tests for allowed_amount_table_2020
--unit tests models have five columns: name of unit test (test_name), result of the test (value), 
--expected value of test (expected_value), whether or not the test passed (test_passed),
--whether or not the error is considered fatal (fatal_error), whether or not the test failed AND
--the error was fatal (fatal_result)

--count check
with count_unit_test as (select
'aa_count' as test_name
,(select count(*) as count from {{ref('allowed_amounts_table_2020')}}) as value
{% if var('limit_level')>0 %}
{% set expected_count=10**(var('limit_level')*2) %}
, {{expected_count}} as expected_value
, (value=expected_value) as test_passed
{% else %}
, NULL as expected_value
, TRUE as test_passed
{% endif %}
, FALSE as fatal_error
, (test_passed and fatal_error) as fatal_result),

--aa below 0 check
aa_belowzero_unit_test as (select
'aa_belowzero_count' as test_name
,(select count(*) as count from {{ref('allowed_amounts_table_2020')}} where allowed_amount<0) as value
,0 as expected_value
, (value=expected_value) as test_passed
, FALSE as fatal_error
, (test_passed and fatal_error) as fatal_result),


--aa above 10000000 check
aa_above10M_unit_test as (select
'aa_above10M_count' as test_name
,(select count(*) as count from {{ref('allowed_amounts_table_2020')}} where allowed_amount>10000000) as value
, 0 as expected_value
, (value=expected_value) as test_passed
, FALSE as fatal_error
, (test_passed and fatal_error) as fatal_result),



--checks unique source array per encounter key 
unique_source_ekey_unit_test as (select
'unique_source_ekey_count' as test_name
,(select count(distinct encounter_key, sources_array) as count from {{ref('allowed_amounts_table_2020')}}) as value
, (select count(*) as count from {{ref('allowed_amounts_table_2020')}}) as expected_value
, (value=expected_value) as test_passed
, FALSE as fatal_error
, (test_passed and fatal_error) as fatal_result)

select * from count_unit_test
union
select * from aa_belowzero_unit_test
union
select * from aa_above10M_unit_test
union
select * from unique_source_ekey_unit_test




