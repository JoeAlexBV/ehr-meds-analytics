-- dbt_semantic_layer/models/fact_diagnoses.sql

{{ config(materialized='external', location='../data/marts/fact_diagnoses.parquet') }}

select
    subject_id as patient_id,
    time as diagnosis_date,
    REPLACE(code, 'ICD10CM/', '') as icd10_code,
    text_value as description
from {{ source('meds', 'event_stream') }}
where code like 'ICD10CM/%'