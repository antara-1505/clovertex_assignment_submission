import os
import pandas as pd
import matplotlib.pyplot as plt
import json

PLOT_DIR = "datalake/consumption/plots/"
os.makedirs(PLOT_DIR, exist_ok=True)


def plot_age_distribution(summary_file):
    df = pd.read_parquet(summary_file)

    age_df = df[df["type"] == "age_group"].copy()

    # ❗ Fix: remove NaN categories
    age_df = age_df.dropna(subset=["category"])

    plt.figure()
    plt.bar(age_df["category"].astype(str), age_df["count"])

    plt.title("Age Distribution")
    plt.xlabel("Age Group")
    plt.ylabel("Number of Patients")

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/age_distribution.png")
    plt.close()


def plot_gender_distribution(summary_file):
    df = pd.read_parquet(summary_file)

    gender_df = df[df["type"] == "gender"].copy()

    # ❗ Fix: handle NaN
    gender_df["category"] = gender_df["category"].fillna("Unknown")

    plt.figure()
    plt.bar(gender_df["category"].astype(str), gender_df["count"])

    plt.title("Gender Distribution")
    plt.xlabel("Gender")
    plt.ylabel("Number of Patients")

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/gender_distribution.png")
    plt.close()

def plot_diagnosis_frequency(
    input_file="datalake/consumption/diagnosis_frequency.parquet"
):
    # ---------------------------
    # 1. Load data
    # ---------------------------
    df = pd.read_parquet(input_file)

    # ---------------------------
    # 2. Clean data
    # ---------------------------
    df = df.dropna(subset=["chapter_name", "patient_count"])

    # Sort ascending for horizontal bar (better visualization)
    df = df.sort_values(by="patient_count", ascending=True)

    # ---------------------------
    # 3. Plot
    # ---------------------------
    plt.figure(figsize=(10, 6))

    plt.barh(df["chapter_name"], df["patient_count"])

    plt.title("Top 15 ICD-10 Chapters by Patient Count")
    plt.xlabel("Number of Patients")
    plt.ylabel("ICD-10 Chapter")

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/diagnosis_frequency.png")
    plt.close()


# lab distributions
def plot_lab_trends(data_file):
   

    df = pd.read_parquet(data_file)

    # Detect value column
    # value_col = "result_value" if "result_value" in df.columns else "value"

    df["date"] = pd.to_datetime(df["collection_date"], errors="coerce")

    # Focus on key tests
    tests = ["hba1c", "creatinine"]

    for test in tests:
        test_df = df[df["test_name"].str.lower() == test].copy()

        if test_df.empty:
            print(f"No data for {test}")
            continue

        # Aggregate mean per date (cleaner than per patient)
        trend_df = (
            test_df.groupby("date")["test_value"]
            .mean()
            .reset_index()
            .sort_values("date")
        )

        # ---------------------------
        # Plot
        # ---------------------------
        plt.figure(figsize=(8, 5))

        plt.plot(trend_df["date"], trend_df['test_value'], marker="o")

        plt.title(f"{test.upper()} Trend Over Time")
        plt.xlabel("Date")
        plt.ylabel("Average Value")

        plt.xticks(rotation=45)
        plt.tight_layout()

        plt.savefig(f"{PLOT_DIR}/{test}_distribution.png")
        plt.close()


def plot_genomics_scatter(data_file):

    df = pd.read_parquet(data_file)

    # ---------------------------
    # Clean data
    # ---------------------------
    df = df.dropna(subset=["allele_frequency", "read_depth", "clinical_significance"])

    # Normalize labels
    df["clinical_significance"] = (
        df["clinical_significance"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # ---------------------------
    # Color mapping
    # ---------------------------
    color_map = {
        "pathogenic": "red",
        "likely pathogenic": "orange",
        "uncertain significance": "gray",
        "benign": "green"
    }

    plt.figure(figsize=(8, 6))

    # Plot each category separately
    for label, color in color_map.items():
        subset = df[df["clinical_significance"] == label]

        if subset.empty:
            continue

        plt.scatter(
            subset["read_depth"],
            subset["allele_frequency"],
            label=label.title(),
            alpha=0.6
        )

    # ---------------------------
    # Formatting
    # ---------------------------
    plt.title("Genomics Scatter: Allele Frequency vs Read Depth")
    plt.xlabel("Read Depth")
    plt.ylabel("Allele Frequency")

    plt.legend()
    plt.tight_layout()

    plt.savefig(f"{PLOT_DIR}/genomics_scatter.png")
    plt.close()


def plot_high_risk_summary(high_risk_file, unified_file):
    # ---------------------------
    # Load data
    # ---------------------------
    high_risk = pd.read_parquet(high_risk_file)
    df = pd.read_parquet(unified_file)

    # ---------------------------
    # Filter high-risk patients
    # ---------------------------
    df_hr = df[df["patient_id"].isin(high_risk["patient_id"])]

    # ---------------------------
    # Prepare distributions
    # ---------------------------
    gender_dist = df_hr["sex"].fillna("Unknown").value_counts()
    site_dist = df_hr["site"].fillna("Unknown").value_counts().head(10)

    total_patients = df_hr["patient_id"].nunique()

    # ---------------------------
    # Plot
    # ---------------------------
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Gender plot
    axes[0].bar(gender_dist.index.astype(str), gender_dist.values)
    axes[0].set_title("Gender Distribution (High-Risk)")
    axes[0].set_xlabel("Gender")
    axes[0].set_ylabel("Count")

    # Site plot
    axes[1].barh(site_dist.index.astype(str), site_dist.values)
    axes[1].set_title("Top Sites (High-Risk)")
    axes[1].set_xlabel("Count")
    axes[1].set_ylabel("Site")

    # Overall title
    fig.suptitle(f"High-Risk Patient Summary (Total: {total_patients})")

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/high_risk_summary.png")
    plt.close()


def plot_data_quality():
    path = "datalake/data_quality_report.json"

    if not os.path.exists(path):
        return

    with open(path, "r") as f:
        data = json.load(f)

    datasets = [d["dataset"] for d in data]
    duplicates = [d["issues_found"]["duplicates_removed"] for d in data]
    nulls = [d["issues_found"]["nulls_handled"] for d in data]

    x = range(len(datasets))

    plt.figure()

    plt.bar(x, duplicates, label="Duplicates Removed")
    plt.bar(x, nulls, bottom=duplicates, label="Nulls Handled")

    plt.xticks(x, datasets, rotation=45)
    plt.title("Data Quality Overview")
    plt.ylabel("Count")

    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/data_quality.png")
    plt.close()


def run_visualizations():
    df = pd.read_parquet("datalake/refined/final_unified_output.parquet")

    plot_age_distribution('datalake/consumption/patient_summary.parquet')
    plot_gender_distribution('datalake/consumption/patient_summary.parquet')
    plot_diagnosis_frequency("datalake/consumption/diagnosis_frequency.parquet")
    plot_lab_trends("datalake/consumption/lab_flagged_data.parquet")
    plot_genomics_scatter("datalake/refined/genomics_filtered.parquet")
    plot_high_risk_summary(
    high_risk_file="datalake/consumption/high_risk_patients.parquet",
    unified_file="datalake/refined/final_unified_output.parquet"
    )
    plot_data_quality()

    print("All required Task 4 plots generated")
