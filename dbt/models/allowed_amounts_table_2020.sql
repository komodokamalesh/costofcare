{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--basic pull of 2020 allowed amounts data 
--NOTE: need to do distinct checks on encounter_key/sources pairing

select
encounter_key
,allowed_amount::float as allowed_amount
,sources as sources_array
,procedure
from {{source('encounters', 'aa')}}
where service_from::date between '2020-01-01' and '2020-12-31'
--throttle query based on limit_level
{% if var('limit_level') > 0 %}
{% set temp_limit = 10**(var('limit_level')*2) %}
limit {{temp_limit}}
{% endif %}
