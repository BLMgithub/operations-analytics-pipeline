# =============================================================================
# Google Drive Extractor Logic
# =============================================================================


from typing import List, Dict
from data_extract.shared.utils import (
    extract_file_content,
    check_handshake,
    upload_to_gcs,
    GoogleDriveService,
)

ARCHIVAL_BUCKET = "gs://operations-archival-bucket"
PIPELINE_BUCKET = "gs://operations-pipeline-bucket"
PARENT_FOLDER = "operations-upload-folder"
MIME_TYPE = "application/vnd.google-apps.folder"


def get_target_folder_id(
    target_folder: str, drive_api: GoogleDriveService
) -> str | None:
    """
    Resolves a specific Drive Folder ID within the strict system hierarchy.

    Contract:
    - Locates the system root 'PARENT_FOLDER'.
    - Performs a scoped search for 'target_folder' strictly within that root.

    Invariants:
    - Namespace Protection: Prevents global Drive collisions by enforcing the
      parent-child relationship.

    Failures:
    - Returns None if the parent root or the target subfolder is missing/inaccessible.
    """

    # Find Root Folder
    parent_query = f"name = '{PARENT_FOLDER}' and mimeType = '{MIME_TYPE}'"
    parent_results = (
        drive_api.files().list(q=parent_query, fields="files(id)").execute()
    )
    parents = parent_results.get("files", [])

    if not parents:
        print(f"[ERROR]: Parent folder '{PARENT_FOLDER}' not found or not shared.")
        return None

    parent_id = parents[0]["id"]

    # Find Target folder inside Parent
    child_query = f"name = '{target_folder}' and '{parent_id}' in parents and mimeType = '{MIME_TYPE}'"
    child_results = drive_api.files().list(q=child_query, fields="files(id)").execute()
    children = child_results.get("files", [])

    if not children:
        print(f"[ERROR]: Folder '{target_folder}' not found inside '{PARENT_FOLDER}'.")
        return None

    return children[0]["id"]


def get_valid_files(
    folder_id: str, target_folder: str, drive_api: GoogleDriveService
) -> List[Dict] | None:
    """
    Retrieves the candidate file manifest from the target Drive folder.

    Contract:
    - Fetches metadata (ID, Name, MimeType) for all files in the parent folder.
    - Filters out system-reserved files (e.g., 'instruction.txt').

    Invariants:
    - Scope: Only direct children of 'folder_id' are returned (non-recursive).

    Failures:
    - Returns an empty list if the folder is empty or contains only reserved files.
    """

    # Validate instruction inside the folder
    if not check_handshake(drive_api, folder_id):
        print(f"[ERROR]: '{target_folder}' missing instruction.txt or upload not safe.")
        return None

    # List files inside folder (excluding instruction.txt)
    file_results = (
        drive_api.files()
        .list(
            q=f"'{folder_id}' in parents and name != 'instruction.txt'",
            fields="files(id, name, mimeType)",
        )
        .execute()
    )

    files_in_drive = file_results.get("files", [])

    # Warn if folder is valid but empty
    if not files_in_drive:
        print(f"[WARNING]: '{target_folder}' is empty. Nothing to process.")

    return files_in_drive


def process_extraction(
    file: dict,
    drive_api: GoogleDriveService,
    archival_path: str,
    pipeline_path: str,
) -> tuple[bool, dict]:
    """
    Executes the dual-mirroring extraction for a single file.

    Contract:
    - Extracts binary content from Google Drive via the service provider.
    - Performs a synchronous dual-upload to the Archival and Pipeline GCS buckets.

    Invariants:
    - Mirroring: Every successful extraction results in two identical GCS
      artifacts in separate administrative zones.
    - Integrity: The extraction fails if either GCS upload fails.

    Returns:
        tuple: (Success Boolean, Metadata/Error Detail Dictionary).
    """

    try:
        data = extract_file_content(drive_api, file["id"], file["mimeType"])

        upload_to_gcs(ARCHIVAL_BUCKET, archival_path, data)
        upload_to_gcs(PIPELINE_BUCKET, pipeline_path, data)

        print(f"[INFO]: File '{file['name']}' extracted successfully.")
        success_details = {"name": file["name"], "status": "success"}

        return True, success_details

    except Exception as e:
        print(f"[ERROR]: Execution halted on file '{file['name']}': {str(e)}")

        error_details = {
            "file_name": file["name"],
            "drive_id": file["id"],
            "error_type": type(e).__name__,
            "error_message": str(e),
        }

        return False, error_details
