{{
  config(
    materialized='table', 
    transient=false
  	)
}}

--pull MS/Ocrelizumb encounters


select 
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
and claim_date between '2017-01-01' and '2021-07-31'


