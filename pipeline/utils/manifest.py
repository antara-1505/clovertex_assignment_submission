import os
import json
import hashlib
import pandas as pd
from datetime import datetime, timezone


def generate_checksum(file_path):
    """Generate MD5 checksum of a file"""
    hash_md5 = hashlib.md5()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def generate_manifest(file_path):
    """Generate manifest metadata for a dataset"""

    df = pd.read_parquet(file_path)

    schema = {col: str(dtype) for col, dtype in df.dtypes.items()}

    manifest = {
        "file_name": os.path.basename(file_path),
        "path": file_path,
        "rows": len(df),
        "columns": len(df.columns),
        "schema": schema,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checksum": generate_checksum(file_path)
    }

    return manifest


def save_manifest(manifest, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    manifest_path = os.path.join(output_dir, "manifest.json")

    # If exists → append
    if os.path.exists(manifest_path):
        with open(manifest_path, "r") as f:
            existing = json.load(f)
    else:
        existing = []

    existing.append(manifest)

    with open(manifest_path, "w") as f:
        json.dump(existing, f, indent=4)