# src/transformers/meds_mapper.py
import polars as pl

class MedsETL:
    def __init__(self, raw_path: str, output_path: str):
        self.raw_path = raw_path
        self.output_path = output_path

    def process_demographics(self) -> pl.DataFrame:
        df = pl.read_parquet(f"{self.raw_path}/patients.parquet")
        
        # MEDS Birth Event
        births = df.select(
            subject_id=pl.col("patient_id").cast(pl.Int64),
            time=pl.col("birth_date").str.to_date().cast(pl.Datetime("us")),
            code=pl.lit("MEDS_BIRTH"),
            numeric_value=pl.lit(None, dtype=pl.Float32),
            text_value=pl.lit(None, dtype=pl.Utf8)
        )
        
        # Gender Event (Static - no time)
        gender = df.select(
            subject_id=pl.col("patient_id").cast(pl.Int64),
            time=pl.lit(None, dtype=pl.Datetime("us")),
            # FIX: Use pl.lit() + pl.col() for concatenation
            code=pl.lit("DEMOGRAPHICS//GENDER/") + pl.col("gender").str.to_uppercase(),
            numeric_value=pl.lit(None, dtype=pl.Float32),
            text_value=pl.lit(None, dtype=pl.Utf8)
        )
        return pl.concat([births, gender])

    def process_diagnoses(self) -> pl.DataFrame:
        df = pl.read_parquet(f"{self.raw_path}/diagnoses.parquet")
        return df.select(
            subject_id=pl.col("patient_id").cast(pl.Int64),
            time=pl.col("diagnosis_date").str.to_date().cast(pl.Datetime("us")),
            # FIX: Use pl.lit() + pl.col() for concatenation
            code=pl.lit("ICD10CM/") + pl.col("icd10_code").str.replace(r"\.", ""),
            numeric_value=pl.lit(None, dtype=pl.Float32),
            text_value=pl.col("description").cast(pl.Utf8)
        )

    def process_labs(self) -> pl.DataFrame:
        df = pl.read_parquet(f"{self.raw_path}/labs.parquet")
        return df.select(
            subject_id=pl.col("patient_id").cast(pl.Int64),
            time=pl.col("result_date").str.to_date().cast(pl.Datetime("us")),
            # FIX: Use pl.lit() + pl.col() for concatenation
            code=pl.lit("LOINC/") + pl.col("loinc_code"),
            numeric_value=pl.col("result_value").cast(pl.Float32),
            text_value=pl.lit(None, dtype=pl.Utf8)
        )

    def run_pipeline(self):
        print("Starting MEDS transformation...")
        events = [
            self.process_demographics(),
            self.process_diagnoses(),
            self.process_labs()
        ]
        
        # Combine everything into the final MEDS format
        meds_df = pl.concat(events)
        
        # Sort by patient and time as required by standard
        meds_df = meds_df.sort(["subject_id", "time"])
        
        output_file = f"{self.output_path}/data.parquet"
        meds_df.write_parquet(output_file)
        print(f"MEDS data successfully written to {output_file}")

if __name__ == "__main__":
    etl = MedsETL(raw_path="data/raw", output_path="data/meds")
    etl.run_pipeline()