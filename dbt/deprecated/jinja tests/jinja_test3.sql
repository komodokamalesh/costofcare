{{
  config(
    materialized='table'
  	)
}}

select array_agg(column_name) as agg_col from information_schema.columns where 
	table_name='ALLOWED_AMOUNTS_TABLE_2020_ENHANCED'