-- dbt_semantic_layer/models/fact_labs.sql

{{ config(materialized='external', location='../data/marts/fact_labs.parquet') }}

select
    subject_id as patient_id,
    time as result_date,
    REPLACE(code, 'LOINC/', '') as loinc_code,
    numeric_value as result_value
from {{ source('meds', 'event_stream') }}
where code like 'LOINC/%'