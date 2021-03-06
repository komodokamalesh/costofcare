{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--summary of medicare vs allowed amounts
--sets up range of acceptable prices based on medicare fees

select
procedure 
,procedure_description
,facility_cat as facility_category
,count(*) as kh_n_obs
,min(ALLOWED_AMOUNT::float) as min_aa
,approx_percentile(ALLOWED_AMOUNT::float,.1) as aa_10_percentile
,avg(ALLOWED_AMOUNT::float) as average_aa
,median(ALLOWED_AMOUNT::float) as median_aa
,approx_percentile(ALLOWED_AMOUNT::float,.9) as aa_90_percentile
,max(ALLOWED_AMOUNT::float) as max_aa
,avg(us_avg_mpfs) as avg_med_fee
--lower bound of fee range
,count_if(allowed_amount<.75*us_avg_mpfs)/count(*) as pct_below_range
--upper bound of fee range
,count_if(allowed_amount>3*us_avg_mpfs)/count(*) as pct_above_range
from {{ref('procedure_price_w_mpfs_summ')}}
where procedure is not null
group by procedure, procedure_description, facility_cat
order by kh_n_obs desc



