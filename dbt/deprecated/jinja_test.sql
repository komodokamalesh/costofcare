
{{
  config(
    materialized='table'
  	)
}}

{%- set table_column_names = get_column_names('ALLOWED_AMOUNTS_TABLE_2020_ENHANCED') -%}


WITH table_count_unit_test as (select
'aa_enhanced_count' as test_name
,(select count(*) as count from {{ref('allowed_amounts_table_2020_enhanced')}}) as value
,(select count(*) as count from {{ref('allowed_amounts_table_2020')}}) as expected_value
, (value=expected_value) as test_passed
, FALSE as fatal_error
, (test_passed and fatal_error) as fatal_result)

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


	
