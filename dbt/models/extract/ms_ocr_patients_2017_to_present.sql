{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--pull MS/Ocrelizumb encounters


with cohort_encounter_keys as (select 
distinct
encounter_key
from {{source('encounters_new', 'mx')}} as h,
lateral flatten(input=>h.service_lines) as s
where ((s.value:procedure='J2350' or  s.value:ndc='50242015001')
and arrays_overlap(array_construct_compact(h.DA,h.D1,h.D2,h.D3,h.D4,h.D5,h.D6,h.D7
    ,h.D8,h.D9,h.D10,h.D11,h.D12,h.D13,h.D14,h.D15
    ,h.D16,h.D17,h.D18,h.D19,h.D20,h.D21,h.D22,h.D23
    ,h.D24,h.D25,h.D26,h.diagnosis_e_code_1,h.diagnosis_e_code_2),
        (array_construct('G35'))))
and claim_date between '2017-01-01' and '2021-07-31'),


--pull encounters data for selected encounters keys
cohort_encounters as (select 
claim_date
,upk_key2
,e.encounter_key
,visit_id
,total_claim_charge_amount
,patient_amount_paid
,payer_kh_id
,bill_type_code
,payer_channel
,statement_from
,service_lines
from {{source('encounters_new', 'mx')}} as e
join cohort_encounter_keys as ek
on e.encounter_key=ek.encounter_key),



--add closed/open flag as calculation on whether PATIENT is closed during claim
closed_flag as (select
c.claim_date,
c.encounter_key, 
c.upk_key2,
(arrays_overlap(array_construct(month(c.claim_date)), e.months_closed_mx)) as closed_flag
from cohort_encounters c
join {{source('encounters_new', 'eligibility')}} as e
on e.upk_key2=c.upk_key2
and year(c.claim_date)=e.year
where e.months_closed_mx is not null),

--aggregate closed flag by claim_date, encounter_key, upk_key2 combination
closed_flag_agg as (select
claim_date, encounter_key, upk_key2,
boolor_agg(closed_flag) as closed_flag
from closed_flag
group by claim_date, encounter_key, upk_key2),

--fold aggregated closed flag into patient table
cohort_encounters_w_closed_flag as (select
ce.*,
ifnull(c.closed_flag,'False') as closed_flag
from cohort_encounters ce
left join (select claim_date, encounter_key, upk_key2, closed_flag 
  from closed_flag_agg where closed_flag) as c on 
c.claim_date=ce.claim_date and c.encounter_key=ce.encounter_key and c.upk_key2=ce.upk_key2)

--unroll service lines
select 
e.claim_date
,upk_key2
,e.encounter_key
,visit_id
,total_claim_charge_amount
,patient_amount_paid
,payer_kh_id
,bill_type_code
,payer_channel
,statement_from
,s.value:line_charge as line_charge
,s.value:date_of_service as date_of_service
,s.value:service_from as sl_service_from  
,s.value:service_to as sl_service_to
,s.value:procedure as procedure
from cohort_encounters_w_closed_flag as e, 
lateral flatten(input=>e.service_lines) as s


