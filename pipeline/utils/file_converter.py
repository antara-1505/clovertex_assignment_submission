import json
import csv
import pyarrow.parquet as pq
import pandas as pd
import os


class PatientDataConverter:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.data = []
        self.csv_output = []

    def load_json(self):
        with open(self.input_file, 'r') as f:
            self.data = json.load(f)

    def transform_data(self):
        for row in self.data:
            dd = {
                'patient_id': row.get('patientID'),
                'first_name': row.get('name', {}).get('given'),
                'last_name': row.get('name', {}).get('family'),
                'date_of_birth': row.get('birthDate'),
                'sex': row.get('gender'),
                'blood_group': row.get('bloodType'),
                'admission_dt': row.get('encounter', {}).get('admissionDate'),
                'discharge_dt': row.get('encounter', {}).get('dischargeDate'),
                'contact_phone': row.get('contact', {}).get('phone'),
                'contact_email': row.get('contact', {}).get('email'),
                'site': row.get('encounter', {}).get('facility')
            }
            self.csv_output.append(dd)

    def write_csv(self):
        if not self.csv_output:
            raise ValueError("No data to write. Run transform_data() first.")

        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.csv_output[0].keys())
            writer.writeheader()
            writer.writerows(self.csv_output)

    def convert_json_to_csv(self):
        self.load_json()
        self.transform_data()
        self.write_csv()


class CSVtoParquetMerger:
    def __init__(self, input_files, output_file):
        """
        input_files: list of CSV file paths
        output_file: output parquet file path
        """
        self.input_files = input_files
        self.output_file = output_file
        self.dataframes = []

    def load_csvs(self):
        for file in self.input_files:
            if not os.path.exists(file):
                raise FileNotFoundError(f"{file} not found")
            df = pd.read_csv(file, dtype={"contact_phone": "string"})
            self.dataframes.append(df)

    def validate_schema(self):
        """
        Ensures all CSVs have the same columns (order-independent)
        """
        base_cols = set(self.dataframes[0].columns)

        for i, df in enumerate(self.dataframes[1:], start=2):
            if set(df.columns) != base_cols:
                raise ValueError(f"Schema mismatch in file {i}")

    def normalize_columns(self):
        """
        Ensures same column order across all dataframes
        """
        cols = self.dataframes[0].columns
        self.dataframes = [df[cols] for df in self.dataframes]

    def merge_data(self):
        return pd.concat(self.dataframes, ignore_index=True)

    def write_parquet(self, merged_df):
        merged_df.to_parquet(self.output_file, index=False, engine="pyarrow")

    def run(self):
        self.load_csvs()
        self.validate_schema()
        self.normalize_columns()
        merged_df = self.merge_data()
        self.write_parquet(merged_df)


class SimpleJSONToCSVConverter:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def convert(self):
        # Read JSON
        with open(self.input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Ensure it's a list
        if isinstance(data, dict):
            data = [data]

        # Get headers from all records
        headers = set()
        for record in data:
            if isinstance(record, dict):
                headers.update(record.keys())

        headers = sorted(headers)

        # Write CSV
        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            for record in data:
                if isinstance(record, dict):
                    writer.writerow(record)


class SimpleParquetToCSVConverter:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def convert(self):
        table = pq.read_table(self.input_file)

        with open(self.output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # Header
            writer.writerow(table.schema.names)

            # Rows
            for batch in table.to_batches():
                writer.writerows(batch.to_pandas().values)
