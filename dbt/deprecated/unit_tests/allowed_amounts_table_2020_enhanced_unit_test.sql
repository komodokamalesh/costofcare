
{{
  config(
    materialized='table'
  	)
}}


-- unit tests for allowed_amount_table_2020 enhanced
--unit tests models have five columns: name of unit test (test_name), result of the test (value), 
--expected value of test (expected_value), whether or not the test passed (test_passed),
--whether or not the error is considered fatal (fatal_error), whether or not the test failed AND
--the error was fatal (fatal_result)

{%- set table_column_names = get_column_names('ALLOWED_AMOUNTS_TABLE_2020_ENHANCED') -%}


--test count of rows: should be same as feeder table (allowed_amounts_table 2020)
WITH table_count_unit_test as (select
'aa_enhanced_count' as test_name
,(select count(*) as count from {{ref('allowed_amounts_table_2020_enhanced')}}) as value
,(select count(*) as count from {{ref('allowed_amounts_table_2020')}}) as expected_value
, (value=expected_value) as test_passed
, FALSE as fatal_error
, (test_passed and fatal_error) as fatal_result)


--test missing % for each column via loop: assumption is 0% missing
{% for column in table_column_names %}

select 
'{{column}}_missing_percent' as test_name
,(select (1-count({{column}})/count(*)) as percent_missing 
	from {{ref('allowed_amounts_table_2020_enhanced')}}) as value
,0 as expected_value
, (value=expected_value) as test_passed
, FALSE as fatal_error
, (test_passed and fatal_error) as fatal_result
 UNION
{% endfor %}

select * from table_count_unit_test




