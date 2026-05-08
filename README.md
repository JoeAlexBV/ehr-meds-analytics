# EHR to MEDS Pipeline & Analytics Lakehouse

An end-to-end Data Engineering and Analytics pipeline that extracts raw Electronic Health Record (EHR) data, standardizes it into the Medical Event Data Standard (MEDS v0.3), and models it into a Parquet-backed Star Schema for Power BI consumption.

## Architecture Overview

This project uses a decoupled, three-tier architecture:

1. **Extraction & Transformation (Python + Polars):** Simulates raw Snowflake EHR data and transforms it into a highly performant, standard MEDS event stream (`data.parquet`).
2. **Semantic Modeling (dbt + DuckDB):** Reads the skinny MEDS event stream and dynamically pivots it into a Star Schema (Fact and Dimension tables), exporting them as modeled Parquet files.
3. **Business Intelligence (Power BI):** Consumes the modeled Parquet files in a Lakehouse pattern to deliver fast, highly-interactive population health analytics.

## Tech Stack
* **Engine:** Python, Polars (Chosen over Pandas for memory efficiency to support 200k+ patient cohorts).
* **Semantic Layer:** dbt Core, DuckDB.
* **Storage Format:** Apache Parquet.
* **Visualization:** Power BI Desktop.
* **Mock Data Generation:** Faker.

## How to Run Locally

### 1. Setup Environment
```bash
python -m venv venv
source venv/bin/activate  # Mac/Linux
# venv/Scripts/activate   # Windows
pip install polars faker dbt-duckdb