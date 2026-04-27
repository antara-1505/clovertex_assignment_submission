import pandas as pd


class PatientDataUnifier:
    """
    A class to join disparate medical datasets into a single unified record.
    Connects labs, diagnoses, medications, and genomics to a patient master list.
    """
    
    def __init__(self, patients_file, labs_file, diagnoses_file, meds_file, genomics_file):
        # Store file paths
        self.files = {
            'patients': patients_file,
            'labs': labs_file,
            'diagnoses': diagnoses_file,
            'medications': meds_file,
            'genomics': genomics_file
        }
        self.unified_df = None

    def load_and_clean(self):
        """Loads all data and standardizes the patient ID column names."""
        print("Loading datasets...")

        # 1. Load Patients (Parquet format)
        # Note: Requires 'pyarrow' or 'fastparquet' installed in your environment
        df_patients = pd.read_parquet(self.files['patients'])
        
        # 2. Load CSVs
        df_labs = pd.read_csv(self.files['labs'])
        df_diagnoses = pd.read_csv(self.files['diagnoses'])
        df_meds = pd.read_csv(self.files['medications'])
        df_genomics = pd.read_parquet(self.files['genomics'])

        # 3. Standardize Patient ID columns
        # Labs and Genomics use 'patient_ref', while others use 'patient_id'
        df_labs = df_labs.rename(columns={'patient_ref': 'patient_id'})
        df_genomics = df_genomics.rename(columns={'patient_ref': 'patient_id'})

        return df_patients, df_labs, df_diagnoses, df_meds, df_genomics

    def create_unified_records(self):
        """Performs the joins to create a single table."""
        # Load the dataframes
        patients_df, labs_df, diagnosis_df, meds_df, genomics_df = self.load_and_clean()
        
        print("Merging data... (Using Left Joins to preserve all patients)")
        
        # We start with the patient list and progressively merge other data
        # 'how=left' ensures we don't lose patients who don't have certain records
        unified = patients_df.merge(diagnosis_df, on='patient_id', how='left')
        
        # Adding suffixes helps identify which file columns came from if they share names
        unified = unified.merge(meds_df, on='patient_id', how='left', suffixes=('', '_med'))
        unified = unified.merge(genomics_df, on='patient_id', how='left', suffixes=('', '_genom'))
        unified = unified.merge(labs_df, on='patient_id', how='left', suffixes=('', '_diag'))
        
        self.unified_df = unified
        print(f"Success! Created unified table with {len(unified)} total rows.")
        return unified

    def join_datasets_and_save_to_csv(self, output_name="unified_patient_records.parquet"):
        """Saves the final joined table to a parquet file."""
        if self.unified_df is not None:
            self.unified_df.to_parquet(output_name, index=False)
            print(f"File saved as: {output_name}")
        else:
            print("Error: No data to save. Run create_unified_records() first.")
