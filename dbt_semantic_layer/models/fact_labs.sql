-- dbt_semantic_layer/models/fact_labs.sql
select
    subject_id as patient_id,
    time as result_date,
    REPLACE(code, 'LOINC/', '') as loinc_code,
    numeric_value as result_value
from {{ source('meds', 'event_stream') }}
where code like 'LOINC/%'