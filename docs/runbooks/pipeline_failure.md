# Runbook: Pipeline Failure

## Symptoms
- Airflow DAG shows red (failed) status
- Alert email received from SNS topic `sentinel-pipeline-alerts`
- CloudWatch alarm `sentinel-glue-job-failure` triggered

## Investigation Steps

### 1. Identify the failed task
- Open Airflow UI → `sentinel_claims_pipeline` → click the failed DAG run
- Click the red task box → **Log** to view the error

### 2. If `sentinel_extraction` failed
- Check if source CSV files are available in Google Drive folder `1b0QIHpwSmbV_jeOHAvrxUeOCR0Shvo9K`
- Check CloudWatch logs: `/aws-glue/jobs/sentinel`
- Verify `gdown` is listed in the Glue job parameters under `--additional-python-modules`
- Check the S3 raw zone for the expected date partition: `s3://sentinel-claims-data/raw/<date>/`

### 3. If `sentinel_transformation` failed
- Check CloudWatch logs: `/aws-glue/jobs/sentinel`
- Verify input files exist in `s3://sentinel-claims-data/raw/<date>/`
- Common cause: schema drift — a source column was renamed or removed. See **Runbook: Schema Drift**

### 4. If `sentinel_loading` failed
- Check CloudWatch logs: `/aws-glue/jobs/sentinel`
- Verify Parquet files exist in `s3://sentinel-claims-data/landing/<date>/`
- Query Redshift for load errors:
  ```sql
  SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 10;
  ```
- Check that `psycopg2-binary` is listed in `--additional-python-modules`

### 5. If `quality_checks` failed
- Read the Airflow task log — it lists each check with PASS/FAIL
- A FAIL means the data doesn't meet expectations. Investigate the specific check:
  - **Row count**: fewer rows than expected — check if loading completed fully
  - **PK uniqueness**: duplicate records loaded — check transformation deduplication logic
  - **FK integrity**: orphaned records — check join logic in loading job
  - **SCD2 integrity**: multiple current records — check SCD2 merge logic

## Recovery

1. Fix the root cause
2. In Airflow, click the failed task → **Clear**
3. The task will re-run from the point of failure

## Escalation
If unresolved after 30 minutes, check CloudWatch logs and Redshift `stl_load_errors` for detailed diagnostics.
