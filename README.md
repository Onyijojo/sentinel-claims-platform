# Sentinel Claims Platform

A cloud-native data pipeline for workers' compensation insurance analytics. Ingests CSV source files from Google Drive, processes them through AWS Glue, and loads a dimensional model into Amazon Redshift Serverless for analysis.

## Architecture

```
Google Drive (CSV)
      │
      ▼
AWS Glue — extraction.py       →  S3 raw zone
AWS Glue — transformation.py   →  S3 landing zone (Parquet)
AWS Glue — loading.py          →  Redshift Serverless (staging → warehouse)
      │
      ▼
Apache Airflow (orchestration + quality checks)
      │
      ▼
Amazon Redshift Serverless
  ├── staging schema   (raw load)
  ├── warehouse schema (dimensional model)
  └── analytics schema (views for reporting)
```

**Architecture and ERD diagrams:** `docs/`

## Quick Start

### Prerequisites
- AWS CLI configured with `sentinel-dev` credentials
- Docker Desktop running
- Terraform >= 1.5.0

### 1. Provision infrastructure
```bash
cd terraform/
terraform init
terraform apply -var="redshift_master_password=<password>" -var="gdrive_folder_id=<id>"
```

### 2. Start Airflow
```bash
cd airflow/
docker-compose up -d
```
Open http://localhost:8080 (admin / admin)

### 3. Set Airflow Variables
Go to **Admin → Variables** and add:

| Key | Value |
|---|---|
| `redshift_endpoint` | Redshift Serverless workgroup endpoint |
| `secret_arn` | AWS Secrets Manager ARN for Redshift credentials |
| `iam_role_arn` | ARN of the Redshift IAM role |
| `gdrive_folder_id` | Google Drive folder ID containing source CSVs |

### 4. Trigger the pipeline
Unpause and trigger the `sentinel_claims_pipeline` DAG.

## Project Structure

```
sentinel-claims-platform/
├── terraform/          # Infrastructure as Code (AWS Glue, S3, Redshift, IAM)
├── glue_jobs/          # PySpark ETL scripts
│   ├── extraction.py   # Download CSVs from Google Drive → S3 raw zone
│   ├── transformation.py # Clean and transform → S3 landing zone (Parquet)
│   └── loading.py      # COPY from S3 landing → Redshift staging → warehouse
├── airflow/
│   └── dags/           # Airflow DAG definitions
├── sql/
│   ├── ddl/            # Schema and table definitions
│   └── quality_checks.sql
├── tests/              # Standalone quality check runner
└── docs/               # Architecture diagrams and runbooks
```

## Data Sources

| File | Entity | Notes |
|---|---|---|
| `claimants.csv` | Claimants | SCD Type 2 tracked |
| `claims_v1.csv` | Claims (legacy) | Missing `claim_severity` column |
| `claims_v2.csv` | Claims (current) | Full schema |
| `employers.csv` | Employers | SCD Type 2 tracked |
| `payments.csv` | Payments | Deduplication applied |
| `policies.csv` | Policies | SCD Type 2 tracked |

## Pipeline Schedule

Runs daily at **06:00 UTC** via Airflow.

Tasks: `extraction → transformation → loading → quality_checks`

## Key Known Data Issues

- ~50% of claims have NULL `policy_id` — these are dropped during transformation
- `claims_v1.csv` lacks `claim_severity` — defaulted to NULL
- `claim_status` inconsistencies — standardised to `open`, `closed`, `denied`
