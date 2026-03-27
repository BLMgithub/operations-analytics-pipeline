# =============================================================================
# Google Drive Extractor Utils
# =============================================================================


import pyparsing

if not hasattr(pyparsing, "DelimitedList"):
    pyparsing.DelimitedList = pyparsing.delimitedList  # type: ignore

from googleapiclient.discovery import build
from google.cloud import storage
from typing import Any, TypeAlias
from datetime import datetime as dt
from zoneinfo import ZoneInfo


GoogleDriveService: TypeAlias = Any

# ------------------------------------------------------------
# INGESTION VALIDATION
# ------------------------------------------------------------


def extract_file_content(
    service: GoogleDriveService,
    file_id: str,
    mime_type: str,
) -> bytes:
    """
    Downloads or exports file content from Google Drive into memory.

    Args:
        service: Authorized Google Drive API service instance.
        file_id: Unique Drive file identifier.
        mime_type: The MIME type of the source file used to determine the extraction method.

    Behavior:
        - If Google Sheet: Uses files().export_media to perform a server-side
        conversion to CSV.
        - If Binary/CSV: Uses files().get_media to retrieve raw bytes.

    Returns:
        bytes: The raw content of the file.
    """

    if mime_type == "application/vnd.google-apps.spreadsheet":
        # for Google Sheets
        request = service.files().export_media(fileId=file_id, mimeType="text/csv")

    else:
        # for other file
        request = service.files().get_media(fileId=file_id)

    return request.execute()


def check_handshake(service: GoogleDriveService, folder_id: str) -> bool:
    """
    Validates the `instruction.txt` guard rail to ensure uploader completion.

    Args:
        service: Authorized Google Drive API service instance.
        folder_id: ID of the date-suffixed directory to scan.

    Returns:
        bool: True if `instruction.txt` exists and contains `file-upload=safe`.
              False if the file is missing or the handshake string is absent.

    Constraints:
        Requires the `text/plain` content to be decoded from bytes for string matching.
    """

    query = f"'{folder_id}' in parents and name = 'instruction.txt'"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        return False

    content = extract_file_content(service, files[0]["id"], "text/plain").decode(
        "utf-8"
    )
    return "file-upload=safe" in content


def get_target_folder_name(folder_name: str):
    """
    Creates target folder name with recent date as suffix (e.g. operations_YYYY_MM_DD).
    """

    pht_now = dt.now(ZoneInfo("Asia/Manila"))
    today = pht_now.strftime("%Y_%m_%d")
    return f"{folder_name}_{today}"


# ------------------------------------------------------------
# API UTILITIES
# ------------------------------------------------------------


def get_drive_service() -> GoogleDriveService:
    """
    Initializes a Google Drive API v3 service object.

    `cache_discovery=False` to suppress library warnings and improve  container startup time
    """
    return build("drive", "v3", cache_discovery=False)


def check_gcs_marking(bucket_name: str, blob_name: str) -> bool:
    """
    Checks for the existence of a success marker in GCS to ensure idempotency.

    Args:
        bucket_name: Name of the archival bucket.
        blob_name: Path to the .success file (e.g., 'status/YYYY_MM_DD.success').

    Returns:
        bool: True if the marker exists (meaning the folder was already processed).
    """

    bucket_name = bucket_name.replace("gs://", "")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(blob_name).exists()


def upload_to_gcs(
    bucket_name: str,
    destination_blob_name: str,
    data: bytes | str,
    content_type: str = "text/csv",
) -> None:
    """
    Upload of raw data CSVs and JSON file to a GCS bucket.

    Args:
        bucket_name: Target GCS bucket.
        destination_blob_name: The full path/prefix for the new object.
        data: Content to upload.
        content_type: MIME type setting for the GCS object metadata.
    """

    bucket_name = bucket_name.replace("gs://", "")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data, content_type=content_type)


def plant_success_flag(bucket_name: str, folder_path: str):
    """
    Uploads an empty _SUCCESS.txt file to signal the pipeline.

    Args:
        bucket_name: Target GCS bucket.
        folder_path: The full path for the success mark
    """

    bucket_name = bucket_name.replace("gs://", "")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(folder_path)

    # Upload an empty string just to create the file
    blob.upload_from_string("")
