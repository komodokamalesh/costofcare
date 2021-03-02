
{{
  config(
    materialized='table'
  	)
}}

{%- set table_column_names = get_column_names('ALLOWED_AMOUNTS_TABLE_2020_ENHANCED') -%}


select to_json('{{table_column_names}}') as var_name

	
