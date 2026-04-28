import pandas as pd
import json
import os


# DATA_PATH = "../datalake/refined/final_unified_output.parquet"

# df = pd.read_parquet(DATA_PATH)


import pandas as pd
import numpy as np


def generate_patient_summary(
    input_file,
    output_file="datalake/consumption/patient_summary.parquet"
):
    df = pd.read_parquet(input_file)

    # ---------------------------
    # 1. AGE PROCESSING
    # ---------------------------
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")
    today = pd.to_datetime("today")

    df["age"] = (today - df["date_of_birth"]).dt.days // 365

    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 18, 30, 45, 60, 100],
        labels=["0-18", "19-30", "31-45", "46-60", "60+"]
    )

    age_dist = (
        df["age_group"]
        .value_counts(dropna=False)
        .reset_index()
    )
    age_dist.columns = ["category", "count"]
    age_dist["type"] = "age_group"

    # ---------------------------
    # 2. GENDER CLEANING + DISTRIBUTION
    # ---------------------------
    def clean_gender(df, patient_col="patient_id", sex_col="sex"):
        s = (
            df[sex_col]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        mapping = {
            "m": "M",
            "male": "M",
            "f": "F",
            "female": "F"
        }

        df[sex_col] = s.map(mapping)

        # Assign one consistent gender per patient
        df[sex_col] = df.groupby(patient_col)[sex_col] \
            .transform(lambda x: x.dropna().iloc[0] if x.notna().any() else np.nan)

        return df

    df = clean_gender(df)

    gender_dist = (
        df["sex"]
        .fillna("Unknown")
        .value_counts()
        .reset_index()
    )
    gender_dist.columns = ["category", "count"]
    gender_dist["type"] = "gender"

    # ---------------------------
    # 3. SITE DISTRIBUTION
    # ---------------------------
    site_dist = (
        df["site"]
        .fillna("Unknown")
        .value_counts()
        .reset_index()
    )
    site_dist.columns = ["category", "count"]
    site_dist["type"] = "site"

    # ---------------------------
    # 4. COMBINE
    # ---------------------------
    final_summary = pd.concat(
        [age_dist, gender_dist, site_dist],
        ignore_index=True
    )

    # ---------------------------
    # 5. SAVE
    # ---------------------------
    final_summary.to_parquet(output_file, index=False)

    return final_summary

def generate_lab_statistics(lab_file, ref_file, output_file="datalake/consumption/lab_statistics.parquet"):
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
    df.to_parquet("datalake/consumption/lab_flagged_data.parquet", index=False)
    trend.to_parquet("datalake/consumption/lab_trends.parquet", index=False)

    return {
        "stats": stats,
        "flagged_data": df,
        "trends": trend
    }


def generate_diagnosis_frequency(
    patient_file,
    icd_ref_file,
    output_file="datalake/consumption/diagnosis_frequency.parquet"
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

    # ---------------------------top
    # 6. Top 15 chapters
    # ---------------------------
    top15 = chapter_counts.head(15)

    # ---------------------------
    # 7. Save output
    # ---------------------------
    top15.to_parquet(output_file, index=False)

    return top15


def generate_variant_hotspots(
    input_file,
    output_file="datalake/consumption/variant_hotspots.parquet"
):
    # ---------------------------
    # 1. Load data
    # ---------------------------
    df = pd.read_parquet(input_file)

    # ---------------------------
    # 2. Keep only relevant variants
    # ---------------------------
    df = df[
        df["clinical_significance"]
        .str.strip()
        .str.lower()
        .isin(["pathogenic", "likely pathogenic"])
    ]

    # ---------------------------
    # 3. Identify top 5 genes
    # ---------------------------
    top_genes = (
        df["gene"]
        .value_counts()
        .head(5)
        .index
    )

    df_top = df[df["gene"].isin(top_genes)]

    # ---------------------------
    # 4. Compute statistics
    # ---------------------------
    result = (
        df_top.groupby("gene")["allele_frequency"]
        .agg(
            variant_count="count",
            mean_allele_frequency="mean",
            p25=lambda x: x.quantile(0.25),
            p75=lambda x: x.quantile(0.75)
        )
        .reset_index()
        .sort_values(by="variant_count", ascending=False)
    )

    # ---------------------------
    # 5. Save output
    # ---------------------------
    result.to_parquet(output_file, index=False)

    return result


def generate_high_risk_patients(
    unified_file,
    output_file="datalake/consumption/high_risk_patients.parquet"
):
    # ---------------------------
    # 1. Load data
    # ---------------------------
    df = pd.read_parquet(unified_file)

    # ---------------------------
    # 2. Identify HbA1c high patients
    # ---------------------------
    # Adjust column names if needed
    hba1c_df = df[
        (df["test_name"].str.lower() == "hba1c") &
        (df["test_value"] > 7.0)
    ]

    hba1c_patients = set(hba1c_df["patient_id"])

    # ---------------------------
    # 3. Identify pathogenic variant patients
    # ---------------------------
    variant_df = df[
        df["clinical_significance"]
        .str.strip()
        .str.lower()
        .isin(["pathogenic", "likely pathogenic"])
    ]

    variant_patients = set(variant_df["patient_id"])

    # ---------------------------
    # 4. Intersection → high-risk
    # ---------------------------
    high_risk_ids = hba1c_patients.intersection(variant_patients)

    # ---------------------------
    # 5. Create output dataframe
    # ---------------------------
    high_risk_df = df[df["patient_id"].isin(high_risk_ids)].copy()

    # Optional: keep only one row per patient
    high_risk_df = high_risk_df[["patient_id"]].drop_duplicates()

    # ---------------------------
    # 6. Save output
    # ---------------------------
    high_risk_df.to_parquet(output_file, index=False)

    return high_risk_df


def detect_anomalies(df):
    anomalies = {}

    # Impossible age
    if "age" in df.columns:
        anomalies["invalid_age"] = df[(df["age"] < 0) | (df["age"] > 120)].shape[0]

    # Negative lab values
    if "test_value" in df.columns:
        anomalies["negative_lab_values"] = df[df["test_value"] < 0].shape[0]

    return anomalies


def save_results(results, filename):
    os.makedirs("datalake/consumption/", exist_ok=True)

    with open(f"datalake/consumption/{filename}.json", "w") as f:
        json.dump(results, f, indent=4)


def run_analytics():
    df = 'datalake/refined/final_unified_output.parquet'
    generate_patient_summary('datalake/refined/patients.parquet')
    generate_lab_statistics(lab_file="data/site_gamma_lab_results.csv", ref_file="data/reference/lab_test_ranges.json")
    generate_diagnosis_frequency(patient_file=df, icd_ref_file="data/reference/icd10_chapters.csv")
    generate_variant_hotspots("datalake/refined/genomics_filtered.parquet")
    generate_high_risk_patients(df)
