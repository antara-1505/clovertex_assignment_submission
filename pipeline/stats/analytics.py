import pandas as pd

# DATA_PATH = "../datalake/refined/final_unified_output.parquet"

# df = pd.read_parquet(DATA_PATH)

def generate_patient_summary(input_file, output_file="patient_summary.parquet"):
    # Load data
    df = pd.read_parquet(input_file)

    # Convert date_of_birth to datetime
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")

    # Calculate age
    today = pd.to_datetime("today")
    df["age"] = (today - df["date_of_birth"]).dt.days // 365

    # 1. Age summary
    age_summary = df["age"].describe()

    # ---------------------------
    # 2. Gender distribution
    # ---------------------------
    gender_summary = df["sex"].value_counts()

    # ---------------------------
    # 3. Site distribution
    # ---------------------------
    site_summary = df["site"].value_counts()

    # ---------------------------
    # 4. Combine summary
    # ---------------------------
    summary = pd.DataFrame({
        "metric": ["total_patients"],
        "value": [len(df)]
    })

    # ---------------------------
    # 5. Save outputs
    # ---------------------------
    summary.to_parquet(output_file, index=False)

    # Optional: save enriched dataset with age
    df.to_parquet("patient_enriched.parquet", index=False)

    return {
        "age_summary": age_summary,
        "gender_summary": gender_summary,
        "site_summary": site_summary,
        "summary": summary
    }

import pandas as pd
import json


def generate_lab_statistics(lab_file, ref_file, output_file="lab_statistics.parquet"):
    # Load data
    df = pd.read_csv(lab_file)
    
    with open(ref_file) as f:
        ref_ranges = json.load(f)

    # Convert date
    df["collection_date"] = pd.to_datetime(df["collection_date"], errors="coerce")
    
    # ---------------------------
    # 1. Compute statistics
    # ---------------------------
    stats = df.groupby("test_name")["test_value"].agg(
        mean="mean",
        median="median",
        std="std"
    ).reset_index()

    # ---------------------------
    # 2. Flag abnormal values
    # ---------------------------
    def flag_value(row):
        test = row["test_name"]
        value = row["test_value"]

        if test not in ref_ranges:
            return "unknown"

        ref = ref_ranges[test]

        if value < ref["critical_low"] or value > ref["critical_high"]:
            return "critical"
        elif value < ref["normal_low"] or value > ref["normal_high"]:
            return "abnormal"
        else:
            return "normal"

    df["flag"] = df.apply(flag_value, axis=1)

    # ---------------------------
    # 3. Trend analysis
    # ---------------------------
    trend_df = df[df["test_name"].isin(["hba1c", "creatinine"])]

    def compute_trend(group):
        group = group.sort_values("collection_date")

        if len(group) < 2:
            return "insufficient_data"

        first = group["test_value"].iloc[0]
        last = group["test_value"].iloc[-1]

        if last > first:
            return "worsening"
        elif last < first:
            return "improving"
        else:
            return "stable"

    trend = (
        trend_df.groupby(["patient_ref", "test_name"])
        .apply(compute_trend)
        .reset_index(name="trend")
    )

    # ---------------------------
    # 4. Save outputs
    # ---------------------------
    stats.to_parquet(output_file, index=False)

    # Optional: save detailed data
    df.to_parquet("lab_flagged_data.parquet", index=False)
    trend.to_parquet("lab_trends.parquet", index=False)

    return {
        "stats": stats,
        "flagged_data": df,
        "trends": trend
    }

def generate_diagnosis_frequency(
    patient_file,
    icd_ref_file,
    output_file="diagnosis_frequency.parquet"
):
    # ---------------------------
    # 1. Load data
    # ---------------------------
    df = pd.read_parquet(patient_file)
    icd_ref = pd.read_csv(icd_ref_file)

    
    # ---------------------------
    # 2. Preprocess ICD codes
    # ---------------------------
    # Extract first 3 characters (standard ICD prefix)
    # df["code_prefix"] = df["icd10_code"].astype(str).str[:3]
    df = df[df["diagnosis_id"].notna()]

    # Clean reference ranges
    icd_ref[["start", "end"]] = icd_ref["code_range"].str.split("-", expand=True)
    icd_ref["start"] = icd_ref["start"].str.strip()
    icd_ref["end"] = icd_ref["end"].str.strip()

    # ---------------------------
    # 3. Map codes to chapters
    # ---------------------------
    def map_chapter(code):
        if pd.isna(code):
            return "Unknown"

        for _, row in icd_ref.iterrows():
            if row["start"] <= code <= row["end"]:
                return row["chapter_name"]

        return "Unknown"

    df["chapter_name"] = df["icd10_code"].apply(map_chapter)

    # ---------------------------
    # 4. Patient-level deduplication
    # ---------------------------
    patient_chapter = df[["patient_id", "chapter_name"]].drop_duplicates()

    # ---------------------------
    # 5. Count patients per chapter
    # ---------------------------
    chapter_counts = (
        patient_chapter.groupby("chapter_name")["patient_id"]
        .nunique()
        .reset_index(name="patient_count")
        .sort_values(by="patient_count", ascending=False)
    )

    # ---------------------------
    # 6. Top 15 chapters
    # ---------------------------
    top15 = chapter_counts.head(15)

    # ---------------------------
    # 7. Save output
    # ---------------------------
    top15.to_parquet(output_file, index=False)

    return top15

def genomics_hotspots(df):
    if "gene" not in df.columns:
        return {}

    return df["gene"].value_counts().head(5).to_dict()

def high_risk_patients(df):
    if "test_name" not in df.columns:
        return pd.DataFrame()

    high_risk = df[
        (df["test_name"] == "HbA1c") &
        (df["test_value"] > 7) &
        (df["clinical_significance"].isin(["Pathogenic", "Likely pathogenic"]))
    ]

    return high_risk[["patient_id"]].drop_duplicates()

def detect_anomalies(df):
    anomalies = {}

    # Impossible age
    if "age" in df.columns:
        anomalies["invalid_age"] = df[(df["age"] < 0) | (df["age"] > 120)].shape[0]

    # Negative lab values
    if "test_value" in df.columns:
        anomalies["negative_lab_values"] = df[df["test_value"] < 0].shape[0]

    return anomalies

import json
import os

# save results
def save_results(results, filename):
    os.makedirs("datalake/consumption/", exist_ok=True)

    with open(f"datalake/consumption/{filename}.json", "w") as f:
        json.dump(results, f, indent=4)

# Main analytics runner
def run_analytics():
    df = pd.read_parquet("datalake/refined/final_unified_output.parquet")

    generate_patient_summary("datalake/refined/patients.parquet")
    generate_lab_statistics(lab_file="data/site_gamma_lab_results.csv", ref_file="data/reference/lab_test_ranges.json")
    generate_diagnosis_frequency(patient_file="datalake/refined/final_unified_output.parquet", icd_ref_file="data/reference/icd10_chapters.csv")

    # results = {
    #     "patient_summary": patient_summary(df),
    #     "lab_statistics": generate_lab_statistics(),
    #     "diagnosis_frequency": diagnosis_frequency(df),
    #     "genomics_hotspots": genomics_hotspots(df),
    #     "anomalies": detect_anomalies(df)
    # }

    # save_results(result, results, "analytics_output")

    # high_risk = high_risk_patients(df)
    # high_risk.to_csv("../datalake/consumption/high_risk_patients.csv", index=False)
    