-- dbt_semantic_layer/models/dim_patients.sql

{{ config(materialized='external', location='../data/marts/dim_patients.parquet') }}

with birth_events as (
    select 
        subject_id as patient_id, 
        time as birth_date
    from {{ source('meds', 'event_stream') }}
    where code = 'MEDS_BIRTH'
),

gender_events as (
    select 
        subject_id as patient_id, 
        REPLACE(code, 'DEMOGRAPHICS//GENDER/', '') as gender
    from {{ source('meds', 'event_stream') }}
    where code like 'DEMOGRAPHICS//GENDER/%'
)

select 
    b.patient_id,
    b.birth_date,
    g.gender
from birth_events b
left join gender_events g on b.patient_id = g.patient_id