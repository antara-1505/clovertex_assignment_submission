import json
import os
from datetime import datetime, timezone


class DataQualityTracker:
    def __init__(self):
        self.reports = []

    def create_log(self, dataset_name, rows_in):
        return {
            "dataset": dataset_name,
            "rows_in": rows_in,
            "rows_out": 0,
            "issues_found": {
                "duplicates_removed": 0,
                "nulls_handled": 0,
                "encoding_fixed": 0
            },
            "processing_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        }

    def finalize_log(self, log, df):
        log["rows_out"] = len(df)
        self.reports.append(log)

    def save(self, path="datalake/data_quality_report.json"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.reports, f, indent=4)