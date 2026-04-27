import logging
import shutil
from utils.get_clean_file_name import get_clean_file_name
from datetime import datetime, timezone

from cleaning.genomics_filter import filter_genomics_variants
from ingestion.join_datasets import PatientDataUnifier
from utils.manifest import generate_manifest, save_manifest
from stats.analytics import run_analytics
from stats.visualization import run_visualizations
from utils.file_converter import (
    PatientDataConverter,
    SimpleJSONToCSVConverter,
    SimpleParquetToCSVConverter
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def move_files_to_datalake(file_path, destination_path="datalake/raw/"):
    # Placeholder for actual file movement logic to a datalake or cloud storage
    logger.info(f"Moving file to datalake: {file_path}")
    shutil.copy(file_path, destination_path)



def convert_patient_data():
    logger.info("Converting site_beta_patients.json to CSV...")
    converter = PatientDataConverter(
        input_file='data/site_beta_patients.json',
        output_file='data/site_beta_patients.csv'
    )
    converter.convert_json_to_csv()

    logger.info("Patient data conversion completed")


import pandas as pd
import logging

logger = logging.getLogger(__name__)


from utils.data_quality import DataQualityTracker

def merge_patient_files():
    logger.info("Starting CSV merge into parquet...")

    files = [
        "data/site_alpha_patients.csv",
        "data/site_beta_patients.csv"
    ]

    tracker = DataQualityTracker()
    cleaned_dfs = []

    for file in files:
        file_name = get_clean_file_name(file)

        logger.info(f"Processing file: {file_name}")

        # ---------------------------
        # LOAD + ENCODING FIX
        # ---------------------------
        try:
            df = pd.read_csv(file, encoding="utf-8")
            df = df.replace(["None", "none", "NULL", "null", ""], pd.NA)
            encoding_fixed = 0
        except UnicodeDecodeError:
            df = pd.read_csv(file, encoding="latin-1")
            encoding_fixed = 1
            logger.warning(f"Encoding fixed for {file_name}")

        log = tracker.create_log(file_name, len(df))
        log["issues_found"]["encoding_fixed"] = encoding_fixed

        # ---------------------------
        # REMOVE DUPLICATES
        # ---------------------------
        before = len(df)
        df = df.drop_duplicates()
        log["issues_found"]["duplicates_removed"] = before - len(df)

        # ---------------------------
        # HANDLE NULLS
        # ---------------------------
        nulls_before = df.isna().sum().sum()

        df = df.fillna("unknown")
        # df["email"] = df["email"].fillna("unknown")
        # df["phone"] = df["phone"].fillna("unknown")

        nulls_after = df.isna().sum().sum()
        log["issues_found"]["nulls_handled"] = int(nulls_before - nulls_after)

        # ---------------------------
        # SAVE CLEANED VERSION
        # ---------------------------
        df.to_parquet(f'datalake/refined/{file_name}.parquet', index=False)

        # ---------------------------
        # FINALIZE LOG
        # ---------------------------
        tracker.finalize_log(log, df)

        cleaned_dfs.append(df)

    # ---------------------------
    # MERGE ALL
    # ---------------------------
    final_df = pd.concat(cleaned_dfs, ignore_index=True)

    output_path = "datalake/refined/patients.parquet"

    final_df.to_parquet(output_path, index=False)

    manifest = generate_manifest(output_path)
    save_manifest(manifest, "datalake/refined/")

    # ---------------------------
    # SAVE DATA QUALITY REPORT
    # ---------------------------
    tracker.save()

    logger.info("Data quality report generated ")


def convert_json_to_csv():
    logger.info("Starting medications JSON conversion...")
    json_converter = SimpleJSONToCSVConverter(
        "data/medications_log.json",
        "data/medications_log.csv"
    )
    json_converter.convert()
    logger.info("Medications JSON conversion completed")


def convert_csv_to_parquet():
    logger.info("Starting lab results parquet conversion...")
    
    parquet_converter = SimpleParquetToCSVConverter(
        "data/site_gamma_lab_results.parquet",
        "data/site_gamma_lab_results.csv"
    )
    parquet_converter.convert()

    parquet_converter = SimpleParquetToCSVConverter(
        "data/genomics_variants.parquet",
        "data/genomics_variants.csv"
    )
    parquet_converter.convert()


    logger.info("Lab results conversion completed")


import os


def normalize_dates(files, date_columns=None):
    """
    Normalize date columns in given CSV files to YYYY-MM-DD format.

    Args:
        files (list): list of file paths
        date_columns (list): optional list of date column names
    """
    for file in files:
        file_name = get_clean_file_name(file)
        print(f"Processing: {file}")

        df = pd.read_csv(file)

        # If no columns provided, try to auto-detect
        if not date_columns:
            date_columns = [
                col for col in df.columns
                if "date" in col.lower()
            ]

        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col],
                    errors="coerce",
                    dayfirst=True  # handles formats like 18-09-2024
                ).dt.strftime("%Y-%m-%d")

        # Moving original file to datalake/raw for traceability
        move_files_to_datalake(file, destination_path="../datalake/raw/")

        # Save back (overwrite)
        df.to_parquet(f"datalake/refined/{file_name}.parquet", index=False)
        
        print(f"Dates normalized in {file}")

def pre_processing():
    logger.info("Preprocessing pipeline started")

    convert_patient_data()
    merge_patient_files()
    convert_json_to_csv()
    convert_csv_to_parquet()
    
    logger.info("Preprocessing pipeline completed successfully")

if __name__ == "__main__":
    
    print("CI test run successful")
    # Begin pre processing to clean and convert all necessary files before joining datasets
    pre_processing()

    genomics_output = "datalake/refined/genomics_filtered.parquet"

    genomics_df, genomics_log = filter_genomics_variants(
        input_path="data/genomics_variants.parquet",
        output_path=genomics_output
    )

    # 🔥 Add manifest
    manifest = generate_manifest(genomics_output)
    save_manifest(manifest, "datalake/refined/")

    # Initialize the unifier with the provided file names
    unifier = PatientDataUnifier(
        patients_file='data/patients.parquet',
        labs_file='data/site_gamma_lab_results.csv',
        diagnoses_file='data/diagnoses_icd10.csv',
        meds_file='data/medications_log.csv',
        genomics_file='data/genomics_variants.parquet'
    )

    # Execute the operation
    final_data = unifier.create_unified_records()
    
    # Save the output
    unifier.join_datasets_and_save_to_csv("datalake/refined/final_unified_output.parquet")
    
    # Preview the first few rows
    print(final_data.head())
    run_analytics()
    run_visualizations()