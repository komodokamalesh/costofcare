
{{
  config(
    materialized='table'
  	)
}}

{%- set table_column_names = get_column_names('ALLOWED_AMOUNTS_TABLE_2020_ENHANCED') -%}

select a({{table_column_names}})::json as var


	
