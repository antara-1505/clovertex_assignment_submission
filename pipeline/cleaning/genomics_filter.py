import pandas as pd
import logging

logger = logging.getLogger(__name__)


def filter_genomics_variants(input_path, output_path):
    logger.info("Starting genomics filtering...")

    # -----------------------------
    # LOAD DATA
    # -----------------------------
    df = pd.read_parquet(input_path)

    log = {
        "dataset": "genomics_variants",
        "rows_in": len(df),
        "issues_found": {
            "duplicates_removed": 0,
            "low_confidence_removed": 0,
            "non_pathogenic_removed": 0
        }
    }

    # -----------------------------
    # REMOVE DUPLICATES
    # -----------------------------
    before = len(df)
    df = df.drop_duplicates()
    log["issues_found"]["duplicates_removed"] = before - len(df)

    # -----------------------------
    # FILTER 1: CLINICAL SIGNIFICANCE
    # -----------------------------
    before = len(df)
    df = df[
    df["clinical_significance"]
    .str.strip()
    .str.lower()
    .isin(["pathogenic", "likely pathogenic"])
    ]
    log["issues_found"]["non_pathogenic_removed"] = before - len(df)

    # -----------------------------
    # FILTER 2: CONFIDENCE SCORE
    # -----------------------------
    if "allele_frequency" in df.columns:
        before = len(df)
        df = df[df["allele_frequency"] > 0.70]
        log["issues_found"]["low_confidence_removed"] = before - len(df)

    # -----------------------------
    # OPTIONAL FILTER: DEPTH
    # -----------------------------
    if "read_depth" in df.columns:
        df = df[df["read_depth"] > 100]

    # -----------------------------
    # SAVE OUTPUT
    # -----------------------------
    df.to_parquet(output_path, index=False)

    log["rows_out"] = len(df)

    logger.info("Genomics filtering completed ")
    logger.info(log)

    return df, log
