import io
import os
import logging
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import boto3


# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()

FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/")
AWS_REGION = os.getenv("AWS_REGION")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -----------------------------
# CLIENTS
# -----------------------------
def get_drive_client():
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH,
        scopes=SCOPES
    )
    return build('drive', 'v3', credentials=credentials)


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


# -----------------------------
# LIST CSV FILES
# -----------------------------
def list_csv_files(service, folder_id):
    query = f"'{folder_id}' in parents and mimeType='text/csv'"

    results = service.files().list(
        q=query,
        fields="files(id, name)"
    ).execute()

    files = results.get('files', [])
    logger.info(f"Found {len(files)} CSV files")

    return files


# -----------------------------
# DOWNLOAD FILE
# -----------------------------
def download_file(service, file_id):
    request = service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()

    downloader = MediaIoBaseDownload(file_stream, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        logger.info(f"Download progress: {int(status.progress() * 100)}%")

    file_stream.seek(0)
    return file_stream


# -----------------------------
# DETERMINE SUBFOLDER
# -----------------------------
def get_subfolder(filename):
    name = filename.lower()

    if "claimant" in name:
        return "claimants/"
    elif "claim" in name:
        return "claims/"
    elif "employer" in name:
        return "employers/"
    elif "payment" in name:
        return "payments/"
    elif "polic" in name:
        return "policies/"
    else:
        return "others/"


# -----------------------------
# UPLOAD FILE
# -----------------------------
def upload_to_s3(s3, file_stream, filename):
    subfolder = get_subfolder(filename)
    s3_key = f"{S3_PREFIX}{subfolder}{filename}"

    try:
        s3.upload_fileobj(file_stream, S3_BUCKET, s3_key)
        logger.info(f"Uploaded: s3://{S3_BUCKET}/{s3_key}")
    except Exception:
        logger.exception(f"Failed upload for {filename}")
        raise


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def run_pipeline():
    logger.info("Starting data ingestion")

    drive = get_drive_client()
    s3 = get_s3_client()

    files = list_csv_files(drive, FOLDER_ID)

    for file in files:
        file_id = file['id']
        filename = file['name']

        logger.info(f"Processing: {filename}")

        file_stream = download_file(drive, file_id)
        upload_to_s3(s3, file_stream, filename)

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    run_pipeline()