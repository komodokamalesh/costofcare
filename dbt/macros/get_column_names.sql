{% macro get_column_names(table_name) %}


{% set col_names_query %}
select column_name as agg_col from information_schema.columns where 
	table_name='{{table_name}}'
	order by column_name
{% endset %}

{% set results = run_query(col_names_query) %}
{% if execute %}
{% set result_list = results.columns[0].values() %}
{% else %}
{% set result_list= [] %}
{% endif %}

{{ return(result_list) }}

{% endmacro %}