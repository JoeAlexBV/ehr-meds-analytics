# src/extractors/mock_data_generator.py
import polars as pl
from faker import Faker
import random

fake = Faker()
Faker.seed(42)

def generate_mock_ehr_data(num_patients=1000):
    print(f"Generating mock data for {num_patients} patients...")
    
    # 1. Generate Patients
    patients = []
    for i in range(1, num_patients + 1):
        patients.append({
            "patient_id": i,
            "gender": random.choice(["M", "F", "Unknown"]),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
            "race": random.choice(["WHITE", "BLACK", "ASIAN", "OTHER"])
        })
    df_patients = pl.DataFrame(patients)
    df_patients.write_parquet("data/raw/patients.parquet")

    # 2. Generate Diagnoses (ICD-10)
    diagnoses = []
    for _ in range(num_patients * 3): # Avg 3 diagnoses per patient
        diagnoses.append({
            "patient_id": random.randint(1, num_patients),
            "diagnosis_date": fake.date_between(start_date='-5y', end_date='today').isoformat(),
            "icd10_code": f"{random.choice(['E11', 'I10', 'J45', 'A12'])}.{random.randint(10, 99)}",
            "description": fake.sentence(nb_words=3)
        })
    df_diagnoses = pl.DataFrame(diagnoses)
    df_diagnoses.write_parquet("data/raw/diagnoses.parquet")

    # 3. Generate Labs
    labs = []
    lab_tests = [
        ("Hemoglobin", "718-7"), ("Glucose", "2345-7"), ("Cholesterol", "2093-3")
    ]
    for _ in range(num_patients * 5): # Avg 5 labs per patient
        test_name, loinc = random.choice(lab_tests)
        labs.append({
            "patient_id": random.randint(1, num_patients),
            "result_date": fake.date_between(start_date='-5y', end_date='today').isoformat(),
            "test_name": test_name,
            "loinc_code": loinc,
            "result_value": round(random.uniform(5.0, 150.0), 2)
        })
    df_labs = pl.DataFrame(labs)
    df_labs.write_parquet("data/raw/labs.parquet")

    print("Mock data generated successfully in data/raw/")

if __name__ == "__main__":
    generate_mock_ehr_data()