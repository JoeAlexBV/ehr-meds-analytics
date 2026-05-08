# MEDS v0.3 Mapping Notes

This document outlines the clinical and technical mapping logic used to transform the source EHR tables into the Medical Event Data Standard (MEDS v0.3) format.

## 1. Schema Enforcement & Types
To guarantee compliance with the MEDS Python validator, the following strict type casting was enforced via Polars across all events:
* `subject_id`: Cast to `Int64`.
* `time`: Cast to `Datetime("us")`.
* `numeric_value`: Cast to `Float32`.
* `text_value`: Cast to `Utf8` (Large String).

## 2. Demographics Mapping (`patients` table)
* **Birth Event:** Created a distinct `MEDS_BIRTH` event using the `birth_date` column. `numeric_value` and `text_value` are enforced as `null`.
* **Gender Event:** Static demographic event. `time` is explicitly set to `null`. 
  * *Code Structure:* `DEMOGRAPHICS//GENDER/` + `gender` (uppercased).

## 3. Diagnoses Mapping (`diagnoses` table)
* **Time:** Mapped from `diagnosis_date`.
* **Codes (ICD-10):** Handled formatting discrepancies by using regex to strip periods from the source `icd10_code` before prefixing.
  * *Code Structure:* `ICD10CM/` + formatted code (e.g., `A12.34` becomes `ICD10CM/A1234`).
* **Text Value:** Source diagnosis `description` is preserved in the `text_value` column. `numeric_value` is `null`.

## 4. Laboratory Mapping (`labs` table)
* **Time:** Mapped from `result_date`.
* **Codes (LOINC):** Currently assumes standard LOINC availability. 
  * *Code Structure:* `LOINC/` + `loinc_code`. 
  * *Note for Phase 1 Data integration:* If LOINC is missing in the live Snowflake schema, fallback logic will be introduced to use `LAB//` + normalized `test_name`.
* **Values:** `result_value` is cast to `Float32` and stored in `numeric_value`.