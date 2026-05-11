# Runbook: Schema Drift

## What is Schema Drift?
Schema drift occurs when the source CSV files contain columns that differ from the expected schema — new columns added, existing columns renamed, or columns removed.

## Detection

Schema drift is detected in two places:

1. **Extraction job** (`extraction.py`) — compares CSV headers against `EXPECTED_SCHEMAS` before uploading to S3
   - Missing columns → hard failure (job stops)
   - Extra columns → warning logged, job continues

2. **Quality checks** — post-load validation catches structural issues that made it through

## Symptoms
- Extraction job fails with: `Schema violation in <file>: missing columns {<column>}`
- Extraction job logs a warning: `Schema drift in <file> — new columns: {<column>}`
- Transformation job fails with a `KeyError` or `AnalysisException` on a column name

## Response by Scenario

### New column added to source
The extraction job will log a warning but continue. The new column will land in S3 raw zone.

Action:
1. Assess whether the new column should be added to the pipeline
2. If yes: add it to `EXPECTED_SCHEMAS` in `extraction.py`, the staging table DDL, and the warehouse table DDL
3. Re-upload `extraction.py` to `s3://sentinel-claims-data/glue-scripts/extraction.py`
4. Run the Redshift DDL to add the column: `ALTER TABLE staging.stg_<table> ADD COLUMN <col> <type>;`
5. Re-trigger the pipeline

### Column removed from source
The extraction job will fail with a missing column error.

Action:
1. Confirm the column removal is intentional with the data source owner
2. If confirmed: remove the column from `EXPECTED_SCHEMAS` in `extraction.py`
3. Decide whether to DROP the column from staging/warehouse or retain it as NULL
4. Update and re-deploy the extraction script
5. Re-trigger the pipeline

### Column renamed in source
Treated as one column removed + one added.

Action: Follow both procedures above for the old and new column names.

## Updating EXPECTED_SCHEMAS

In `glue_jobs/extraction.py`, update the relevant entry:

```python
EXPECTED_SCHEMAS = {
    "claimants.csv": [
        "claimant_id", "first_name", ...  # add/remove column names here
    ],
    ...
}
```

Then re-upload:
```bash
aws s3 cp glue_jobs/extraction.py s3://sentinel-claims-data/glue-scripts/extraction.py
```
