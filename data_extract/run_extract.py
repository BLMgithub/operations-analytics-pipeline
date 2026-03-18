# =============================================================================
# GOOGLE DRIVE FILE EXTRACTOR
# =============================================================================

import sys
from datetime import datetime as dt
from zoneinfo import ZoneInfo
import json
import uuid
from data_extract.shared.utils import (
    extract_file_content,
    check_handshake,
    get_drive_service,
    check_gcs_marking,
    upload_to_gcs,
)

ARCHIVAL_BUCKET = "gs://operations-archival-bucket"
PIPELINE_BUCKET = "gs://operations-pipeline-bucket"
PARENT_FOLDER = "operations-upload-folder"
MIME_TYPE = "application/vnd.google-apps.folder"


def run_extraction(target_child_folder):

    service = get_drive_service()
    metadata_path = f"logs/{target_child_folder}_metadata.json"
    marking_path = f"status/{target_child_folder}.success"

    # Root Folder
    parent_query = f"name = '{PARENT_FOLDER}' and mimeType = '{MIME_TYPE}'"
    parent_results = service.files().list(q=parent_query, fields="files(id)").execute()
    parents = parent_results.get("files", [])

    if not parents:
        print(f"[ERROR]: Parent folder '{PARENT_FOLDER}' not found or not shared.")

        return 1

    parent_id = parents[0]["id"]

    # Uploaded folder with data
    child_query = f"name = '{target_child_folder}' and '{parent_id}' in parents and mimeType = '{MIME_TYPE}'"
    child_results = service.files().list(q=child_query, fields="files(id)").execute()
    children = child_results.get("files", [])

    if not children:
        print(
            f"[ERROR]: Dated folder {target_child_folder} not found inside {PARENT_FOLDER}."
        )

        return 1

    folder_id = children[0]["id"]

    # Deduplication Check
    if check_gcs_marking(ARCHIVAL_BUCKET, marking_path):
        print(f"[INFO]: {target_child_folder} already processed.")

        return 0

    # Find Folder ID by Name
    query = f"name = '{target_child_folder}' and mimeType = '{MIME_TYPE}'"
    folder_results = service.files().list(q=query, fields="files(id)").execute()
    folders = folder_results.get("files", [])

    if not folders:
        print(f"[ERROR]: Folder {target_child_folder} not found.")

        return 1

    folder_id = folders[0]["id"]

    # Validate upload instruction
    if not check_handshake(service, folder_id):
        print(
            f"[ERROR]: {target_child_folder} missing instruction.txt or upload not safe."
        )

        return 1

    # List files in folder
    file_results = (
        service.files()
        .list(
            q=f"'{folder_id}' in parents and name != 'instruction.txt'",
            fields="files(id, name, mimeType)",
        )
        .execute()
    )

    files_in_drive = file_results.get("files", [])

    metadata = {
        "execution_id": str(uuid.uuid4()),
        "files_processed": [],
        "errors": [],
        "status": "success",
    }

    for file in files_in_drive:

        archival_path = f"{target_child_folder}/{file['name']}.csv"
        pipeline_raw_path = f"raw/{file['name']}.csv"

        try:
            data = extract_file_content(service, file["id"], file["mimeType"])

            # upload directory to archival
            upload_to_gcs(ARCHIVAL_BUCKET, archival_path, data)

            # Upload on pipeline/raw/
            upload_to_gcs(PIPELINE_BUCKET, pipeline_raw_path, data)

            metadata["files_processed"].append(
                {"name": file["name"], "status": "success"}
            )

            print(f"[INFO]: {target_child_folder} processed successfully.")

        except Exception as e:

            error_details = {
                "file_name": file["name"],
                "drive_id": file["id"],
                "error_type": type(e).__name__,
                "error_message": str(e),
            }

            metadata["errors"].append(error_details)
            metadata["status"] = "failed"

            # runtime metadata
            upload_to_gcs(ARCHIVAL_BUCKET, metadata_path, json.dumps(metadata))

            print(f"[ERROR]: Execution halted {str(e)}")
            return 1

    upload_to_gcs(ARCHIVAL_BUCKET, metadata_path, json.dumps(metadata))
    upload_to_gcs(ARCHIVAL_BUCKET, marking_path, "")
    return 0


def get_target_folder_name():

    # Uploaded folder name pattern: "operations_YYYY_MM_DD"
    pht_now = dt.now(ZoneInfo("Asia/Manila"))
    today = pht_now.strftime("%Y_%m_%d")
    return f"operations_{today}"


def main():

    target_folder = get_target_folder_name()
    print(f"[INFO]: Starting extraction for {target_folder}")

    exit_code = run_extraction(target_folder)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

# =============================================================================
# END OF SCRIPT
# =============================================================================
