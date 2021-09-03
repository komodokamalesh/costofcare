{{
  config(
    materialized='table', 
    transient=false
  	)
}}


--pull MS/Ocrelizumb patients



with patient_table as (select upk_key2
from {{source('encounters', 'mx')}} as h,
lateral flatten(input=>h.service_lines) as s
where (s.value:procedure='J2350' 
or arrays_overlap(array_construct_compact(h.DA,h.D1,h.D2,h.D3,h.D4,h.D5,h.D6,h.D7
    ,h.D8,h.D9,h.D10,h.D11,h.D12,h.D13,h.D14,h.D15
    ,h.D16,h.D17,h.D18,h.D19,h.D20,h.D21,h.D22,h.D23
    ,h.D24,h.D25,h.D26,h.diagnosis_e_code_1,h.diagnosis_e_code_2),
        (array_construct('G35')))
or s.value:ndc='50242015001')
and claim_date between {{var('analysis_start_date')}} and {{var('analysis_end_date')}})

select distinct (p.upk_key2)
from patients_table p
join {{source('encounters','eligibility')}} as e
on e.upk_key2=p.upk_key2
where e.year=2020 and array_intersection (array_construct(1,2), months_closed_mx)=array_construct(1,2)