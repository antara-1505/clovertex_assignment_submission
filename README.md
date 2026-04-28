# Clovertex Assignment Submission

A comprehensive healthcare data pipeline that ingests, processes, and analyzes patient data from multiple sources including clinical notes, diagnoses, medications, lab results, and genomic variants.

## 📋 Project Overview

This project implements an end-to-end data pipeline designed to:

- **Ingest** patient data from multiple healthcare sites and formats (CSV, JSON, Parquet)
- **Clean & Transform** data with quality checks, deduplication, and normalization
- **Unify** heterogeneous datasets into a single integrated patient record
- **Analyze** patient cohorts, lab trends, and diagnostic patterns
- **Visualize** insights through statistical graphics and trend analysis

The pipeline follows a layered architecture with **raw**, **refined**, and **consumption** data zones in the datalake.

## 🏗️ Project Structure

```
clovertex_assignment_submission/
├── data/                           # Source datasets
│   ├── site_alpha_patients.csv     # Patient data from Site Alpha
│   ├── site_beta_patients.csv/.json # Patient data from Site Beta (dual format)
│   ├── site_gamma_lab_results.csv/.parquet # Lab test results from Site Gamma
│   ├── diagnoses_icd10.csv         # ICD-10 diagnosis codes and metadata
│   ├── medications_log.csv/.json   # Medication administration logs
│   ├── clinical_notes_metadata.csv # Clinical notes metadata
│   ├── genomics_variants.csv/.parquet # Genomic variant data
│   ├── patients.parquet            # Consolidated patient master data
│   └── reference/                  # Reference data and lookups
│
├── pipeline/                       # Main processing pipeline
│   ├── main.py                     # Pipeline orchestrator and entry point
│   ├── ingestion/                  # Data unification and joining
│   │   └── join_datasets.py        # PatientDataUnifier class for dataset merging
│   ├── cleaning/                   # Data cleaning and filtering
│   │   └── genomics_filter.py      # Genomic variant filtering logic
│   ├── stats/                      # Analytics and statistical analysis
│   │   ├── analytics.py            # Statistical computations and aggregations
│   │   └── visualization.py        # Visualization generation
│   ├── utils/                      # Utility modules
│   │   ├── data_quality.py         # DataQualityTracker for quality metrics
│   │   ├── file_converter.py       # Data format conversion utilities
│   │   ├── get_clean_file_name.py  # Filename sanitization
│   │   └── manifest.py             # Dataset manifest generation
│   ├── *.parquet                   # Generated intermediate outputs
│   └── output.parquet              # Final merged dataset
│
├── datalake/                       # Data lake structure
│   ├── raw/                        # Raw ingested data copies
│   ├── refined/                    # Cleaned and transformed data
│   └── consumption/
│       └── plots/                  # Generated visualizations
│
├── Dockerfile                      # Docker container configuration
├── docker-compose.yml              # Multi-container orchestration
├── requirements.txt                # Python dependencies
└── .gitignore                      # Git ignore rules
```

## 🔧 Technology Stack

- **Language**: Python 3.10+
- **Data Processing**: 
  - `pandas` - Data manipulation and analysis
  - `numpy` - Numerical computations
  - `pyarrow` - Parquet file handling
- **Visualization**: 
  - `matplotlib` - Static plots and graphics
  - `seaborn` - Statistical data visualization
- **Containerization**: Docker & Docker Compose

## 📦 Dependencies

```
pandas
numpy
matplotlib
pyarrow
seaborn
```

Install dependencies:
```bash
pip install -r requirements.txt
```

## 🚀 Getting Started

### Local Execution

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the pipeline**:
   ```bash
   python pipeline/main.py
   ```

The pipeline will:
- Convert JSON patient data to CSV format
- Merge multiple patient files with quality tracking
- Convert medication and genomics data formats
- Normalize date fields across datasets
- Filter and clean genomic variants
- Unify disparate patient datasets
- Generate analytics and visualizations

### Docker Execution

1. **Build and run with Docker Compose**:
   ```bash
   docker-compose up --build
   ```

The container automatically:
- Installs all dependencies
- Creates the datalake directory structure (`datalake/raw`, `datalake/refined`, `datalake/consumption/plots`)
- Executes the pipeline

2. **Access outputs** in the mounted `datalake/` directory on your host machine

## 📊 Pipeline Workflow

```
┌─────────────────────────────────────────────────────────────┐
│                    RAW DATA INGESTION                        │
├─────────────────────────────────────────────────────────────┤
│ • Patient data (Alpha, Beta sites)                           │
│ • Lab results & genomic variants                             │
│ • Medications & diagnoses logs                               │
│ • Clinical notes metadata                                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                  DATA PREPROCESSING                          │
├─────────────────────────────────────────────────────────────┤
│ • Format conversion (JSON → CSV, Parquet → CSV)              │
│ • Encoding fixes (UTF-8 → Latin-1 fallback)                 │
│ • Deduplication & null handling                              │
│ • Date normalization (YYYY-MM-DD format)                     │
│ • Data quality tracking & reporting                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                   DATA REFINEMENT                            │
├─────────────────────────────────────────────────────────────┤
│ • Genomic variant filtering (VAF, coverage thresholds)       │
│ • Patient master data consolidation                          │
│ • Lab result normalization                                   │
│ • Manifest generation for traceability                       │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                   DATA UNIFICATION                           │
├─────────────────────────────────────────────────────────────┤
│ • Join patients ⟷ labs ⟷ diagnoses ⟷ meds ⟷ genomics      │
│ • Multi-key joins on patient/encounter identifiers           │
│ • Create unified patient records                             │
│ • Output final dataset                                       │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                 ANALYTICS & INSIGHTS                         │
├─────────────────────────────────────────────────────────────┤
│ • Lab statistics & trend analysis                            │
│ • Diagnosis frequency distribution                           │
│ • Patient cohort summaries                                   │
│ • Flagged data identification                                │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                  VISUALIZATION                               │
├─────────────────────────────────────────────────────────────┤
│ • Generate charts and plots                                  │
│ • Save visualizations to datalake/consumption/plots/         │
│ • Statistical graphics for key insights                      │
└─────────────────────────────────────────────────────────────┘
```

## 🔍 Key Features

### Data Quality Tracking
- `DataQualityTracker` class monitors:
  - Encoding issues and fixes
  - Duplicates removed
  - Null values handled
  - Row counts before/after transformation

### Multi-Format Support
- **CSV** - Standard tabular data
- **JSON** - Semi-structured patient and medication records
- **Parquet** - Efficient columnar storage

### Data Standardization
- Automatic date normalization across multiple formats
- Null value standardization (None, NULL, "null" → consistent handling)
- Duplicate removal and encoding detection

### Unified Patient Records
- `PatientDataUnifier` class merges:
  - Patient demographics
  - Lab test results
  - Diagnoses (ICD-10)
  - Medications administered
  - Genomic variants

### Analytics Pipeline
- Lab statistics (mean, std, quartiles)
- Diagnosis frequency analysis
- Lab value trends over time
- Flagged abnormal results

## 📁 Output Artifacts

Generated files in `datalake/`:

- **raw/** - Raw data copies for audit trail
- **refined/** - Cleaned datasets
  - `patients.parquet` - Consolidated patient master
  - `genomics_filtered.parquet` - Filtered variants
  - `final_unified_output.parquet` - Complete unified dataset
  - Manifest files for data lineage
- **consumption/plots/** - Visualization outputs

## ⚙️ Configuration

### Modify Data Paths
Edit `pipeline/main.py` to change source file locations:
```python
PatientDataUnifier(
    patients_file='data/patients.parquet',
    labs_file='data/site_gamma_lab_results.csv',
    diagnoses_file='data/diagnoses_icd10.csv',
    meds_file='data/medications_log.csv',
    genomics_file='data/genomics_variants.parquet'
)
```

### Adjust Date Columns
Modify the `date_columns` parameter in `normalize_dates()`:
```python
normalize_dates(files, date_columns=['encounter_date', 'lab_date', 'medication_date'])
```

## 📝 Logging

The pipeline generates detailed logs with:
- Timestamp for each operation
- Operation details (files processed, rows transformed, quality metrics)
- Warning messages for encoding issues or data inconsistencies
- Info messages for successful completion

## 🐳 Docker Details

### Dockerfile
- **Base Image**: `python:3.10-slim`
- **Working Directory**: `/app`
- **Volume Mount**: Current directory mounted to `/app` for data persistence
- **Command**: Automatically runs `python pipeline/main.py`

### docker-compose.yml
- Service: `clovertex_pipeline`
- Volume persistence: `.:/app` ensures outputs are saved to host
- Auto-build from Dockerfile

## 📊 Language Composition

This repository is composed of:
- **Python**: 99.2% - Core pipeline logic and data processing
- **Dockerfile**: 0.8% - Container configuration

## 📝 Recent Updates

**Last Updated**: April 28, 2026

Recent improvements and updates to the repository:
- Enhanced pipeline efficiency and data processing capabilities
- Improved documentation and configuration examples
- Refined data quality tracking and error handling
- Updated dependency specifications

## 🤝 Contributing

1. Ensure all data files are in the `data/` directory
2. Update `requirements.txt` if adding dependencies
3. Follow logging patterns in existing modules
4. Test locally before running in Docker

## 📄 License

This is an assignment submission for Clovertex.

## 🆘 Troubleshooting

**Issue**: Encoding errors in CSV files
- **Solution**: Pipeline automatically attempts UTF-8 first, then falls back to Latin-1

**Issue**: Missing datalake directories
- **Solution**: Docker creates directories automatically; locally run `mkdir -p datalake/{raw,refined,consumption/plots}`

**Issue**: Missing dependencies
- **Solution**: Run `pip install -r requirements.txt`

**Issue**: Memory issues with large datasets
- **Solution**: Process data in chunks using Parquet format which supports streaming

## 📞 Support

For issues or questions about the pipeline, review:
- Pipeline logs for error details
- Data quality report in `datalake/refined/`
- Manifest files for data lineage information
