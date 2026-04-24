"""
Glue Job — Extraction
Google Drive Folder → S3 Raw Zone

Job type: Python Shell (boto3 + gdown, no Spark needed)

Downloads all CSV files from a shared Google Drive folder and uploads them
to the S3 raw zone with date partitioning and MD5 checksum tagging.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WHERE TO ADD YOUR GOOGLE DRIVE FOLDER ID
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
In the Glue job → Job details → Job parameters, add ONE entry:

  Key:   --gdrive_folder_id
  Value: <your folder ID>

How to find the folder ID from a Google Drive folder link:
  Link:  https://drive.google.com/drive/folders/1b0QIHpwSmbV_jeOHAvrxUeOCR0Shvo9K
  ID:    1b0QIHpwSmbV_jeOHAvrxUeOCR0Shvo9K
  (everything after /folders/)

The folder must be shared as "Anyone with the link can view".

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OPTIONAL PARAMETER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  --ingestion_date  YYYY-MM-DD   (omit to default to today's date)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GLUE SETUP
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Job type:                Python Shell
Python version:          3.9
Additional Python libs:  gdown

Upload this script:
  aws s3 cp glue_jobs/extraction.py s3://sentinel-claims-data/glue-scripts/extraction.py

Output:  s3://sentinel-claims-data/raw/{entity}/{ingestion_date}/{file}.csv
"""
import hashlib
import logging
import os
import sys
import tempfile
from datetime import datetime

import boto3
import gdown

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("sentinel.extraction")

BUCKET = "sentinel-claims-data"

# ── Job parameters ────────────────────────────────────────────────────────────

def _get_arg(name: str, default=None):
    """Read --name value from sys.argv (works in Glue Python Shell)."""
    key = f"--{name}"
    if key in sys.argv:
        return sys.argv[sys.argv.index(key) + 1]
    return default

GDRIVE_FOLDER_ID = _get_arg("gdrive_folder_id")
if not GDRIVE_FOLDER_ID:
    raise RuntimeError(
        "Missing required job parameter: --gdrive_folder_id\n"
        "Add it in Glue job → Job details → Job parameters:\n"
        "  Key:   --gdrive_folder_id\n"
        "  Value: <your Google Drive folder ID>"
    )

INGESTION_DATE = _get_arg("ingestion_date", datetime.now().strftime("%Y-%m-%d"))

# ── Entity → file mapping ─────────────────────────────────────────────────────

ENTITIES = {
    "claimants": ["claimants.csv"],
    "claims":    ["claims_v1.csv", "claims_v2.csv"],
    "employers": ["employers.csv"],
    "payments":  ["payments.csv"],
    "policies":  ["policies.csv"],
}

# Flat lookup: filename → entity (built from ENTITIES above)
FILE_ENTITY = {
    fname: entity
    for entity, files in ENTITIES.items()
    for fname in files
}

EXPECTED_SCHEMAS = {
    "claimants.csv": [
        "claimant_id", "first_name", "last_name", "date_of_birth",
        "gender", "employment_start_date", "employer_id", "created_at", "updated_at",
    ],
    "employers.csv": [
        "employer_id", "company_name", "industry", "location",
        "policy_id", "created_at", "updated_at",
    ],
    "payments.csv": [
        "payment_id", "claim_id", "payment_date",
        "payment_amount", "payment_type", "created_at",
    ],
    "policies.csv": [
        "policy_id", "policy_number", "coverage_type",
        "start_date", "end_date", "premium_amount", "created_at", "updated_at",
    ],
    "claims_v1.csv": [
        "claim_id", "claimant_id", "policy_id", "incident_date",
        "report_date", "claim_type", "claim_status",
        "claim_amount", "approved_amount", "created_at", "updated_at",
    ],
    "claims_v2.csv": [
        "claim_id", "claimant_id", "policy_id", "incident_date",
        "report_date", "claim_type", "claim_status",
        "claim_amount", "approved_amount", "created_at", "updated_at",
        "claim_severity",
    ],
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _validate_header(content: bytes, filename: str) -> None:
    header = content.split(b"\n")[0].decode("utf-8")
    actual = [c.strip() for c in header.split(",")]
    expected = EXPECTED_SCHEMAS.get(filename)
    if not expected:
        return
    missing = set(expected) - set(actual)
    extra   = set(actual) - set(expected)
    if missing:
        raise ValueError(f"Schema violation in {filename}: missing columns {missing}")
    if extra:
        log.warning("Schema drift in %s — new columns: %s", filename, extra)


def upload_to_s3(s3_client, content: bytes, entity: str, filename: str) -> None:
    """Validate schema, compute checksum, and write file to the S3 raw zone."""
    _validate_header(content, filename)
    checksum = hashlib.md5(content).hexdigest()
    dest_key = f"raw/{entity}/{INGESTION_DATE}/{filename}"
    s3_client.put_object(
        Bucket=BUCKET,
        Key=dest_key,
        Body=content,
        Tagging=(
            f"checksum={checksum}"
            f"&ingestion_date={INGESTION_DATE}"
            f"&source=google_drive"
        ),
    )
    log.info("Uploaded  s3://%s/%s  [md5=%s]", BUCKET, dest_key, checksum)

# ── Main ──────────────────────────────────────────────────────────────────────

log.info("Starting extraction — ingestion date: %s", INGESTION_DATE)
log.info("Downloading from Google Drive folder: %s", GDRIVE_FOLDER_ID)

s3 = boto3.client("s3")

with tempfile.TemporaryDirectory() as tmp:
    # Download entire folder contents into tmp/
    gdown.download_folder(
        id=GDRIVE_FOLDER_ID,
        output=tmp,
        quiet=False,
        use_cookies=False,
    )

    downloaded = os.listdir(tmp)
    log.info("Files downloaded from Drive: %s", downloaded)

    # Process only the files this job expects
    for fname, entity in FILE_ENTITY.items():
        fpath = os.path.join(tmp, fname)
        if not os.path.exists(fpath):
            log.warning("Expected file not found in Drive folder: %s — skipping", fname)
            continue
        with open(fpath, "rb") as fh:
            content = fh.read()
        upload_to_s3(s3, content, entity, fname)

log.info("Extraction complete — files staged to s3://%s/raw/%s/", BUCKET, INGESTION_DATE)
