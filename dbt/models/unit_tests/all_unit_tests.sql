{{
  config(
    materialized='table', 
  	)
}}

select * from {{ref('allowed_amounts_table_2020_unit_test')}}
union
select * from {{ref('allowed_amounts_table_2020_enhanced_unit_test')}}