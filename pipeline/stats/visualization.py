import os
import pandas as pd
import matplotlib.pyplot as plt

PLOT_DIR = "datalake/consumption/plots/"
os.makedirs(PLOT_DIR, exist_ok=True)

# def plot_patient_demographics(df):
#     # Age distribution
#     if "age" in df.columns:
#         plt.figure()
#         df["age"].dropna().astype(float).hist()
#         plt.title("Age Distribution")
#         plt.xlabel("Age")
#         plt.ylabel("Frequency")
#         plt.tight_layout()
#         plt.savefig(f"{PLOT_DIR}/age_distribution.png")
#         plt.close()

#     # Gender split
#     if "gender" in df.columns:
#         plt.figure()
#         df["gender"].value_counts().plot(kind="bar")
#         plt.title("Gender Distribution")
#         plt.xlabel("Gender")
#         plt.ylabel("Count")
#         plt.tight_layout()
#         plt.savefig(f"{PLOT_DIR}/gender_distribution.png")
#         plt.close()

def plot_patient_demographics(df):
    if "age" in df.columns:
        plt.figure()
        df["age"].dropna().astype(float).hist()
        plt.title("Age Distribution")
        plt.savefig(f"{PLOT_DIR}/age_distribution.png")
        plt.close()

    if "gender" in df.columns:
        plt.figure()
        df["gender"].value_counts().plot(kind="bar")
        plt.title("Gender Distribution")
        plt.savefig(f"{PLOT_DIR}/gender_distribution.png")
        plt.close()

    # Top 15 ICD-10 chapters
def plot_diagnosis_frequency(df):
    if "diagnosis_code" not in df.columns:
        return

    plt.figure()
    df["diagnosis_code"].value_counts().head(15).sort_values().plot(kind="barh")

    plt.title("Top 15 Diagnosis Codes")
    plt.savefig(f"{PLOT_DIR}/diagnosis_frequency.png")
    plt.close()


# lab distributions
def plot_lab_distribution(df):
    if "test_name" not in df.columns:
        return

    df["test_value"] = pd.to_numeric(df["test_value"], errors="coerce")

    tests = df["test_name"].dropna().unique()[:2]

    for test in tests:
        subset = df[df["test_name"] == test]["test_value"].dropna()

        if subset.empty:
            continue

        plt.figure()
        subset.hist()

        # simple reference overlay (mean)
        plt.axvline(subset.mean(), linestyle="dashed", label="Mean")

        plt.title(f"{test} Distribution")
        plt.legend()

        plt.savefig(f"{PLOT_DIR}/{test}_distribution.png")
        plt.close()


def plot_genomics_scatter(df):
    required_cols = ["allele_frequency", "read_depth", "clinical_significance"]

    if not all(col in df.columns for col in required_cols):
        print("⚠️ Missing genomics columns — skipping plot")
        return

    # Convert numeric safely
    df["allele_frequency"] = pd.to_numeric(df["allele_frequency"], errors="coerce")
    df["read_depth"] = pd.to_numeric(df["read_depth"], errors="coerce")

    # Drop rows with missing critical values
    plot_df = df.dropna(subset=["allele_frequency", "read_depth", "clinical_significance"])

    if plot_df.empty:
        print("⚠️ No valid genomics data — skipping plot")
        return

    # Map colors safely
    color_map = {
        "Pathogenic": "red",
        "Likely pathogenic": "orange",
        "Benign": "green"
    }

    colors = plot_df["clinical_significance"].map(color_map)

    # Replace unmapped / NaN colors with default
    colors = colors.fillna("gray")

    plt.figure()

    plt.scatter(
        plot_df["allele_frequency"],
        plot_df["read_depth"],
        c=colors,
        alpha=0.6
    )

    plt.title("Genomics: Allele Frequency vs Read Depth")
    plt.xlabel("Allele Frequency")
    plt.ylabel("Read Depth")

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/genomics_scatter.png")
    plt.close()

def plot_high_risk_summary(df):
    if "test_name" not in df.columns:
        return

    df["test_value"] = pd.to_numeric(df["test_value"], errors="coerce")

    high_risk = df[
        (df["test_name"] == "HbA1c") &
        (df["test_value"] > 7) &
        (df["clinical_significance"].isin(["Pathogenic", "Likely pathogenic"]))
    ]

    if high_risk.empty:
        return

    plt.figure()
    high_risk["site"].value_counts().plot(kind="bar")

    plt.title("High-Risk Patients by Site")
    plt.savefig(f"{PLOT_DIR}/high_risk_summary.png")
    plt.close()


import json

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

    plot_patient_demographics(df)
    plot_diagnosis_frequency(df)
    plot_lab_distribution(df)
    plot_genomics_scatter(df)
    plot_high_risk_summary(df)
    plot_data_quality()

    print("All required Task 4 plots generated")
    