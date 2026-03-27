# =============================================================================
# Google Drive Extractor Orchestrator
# =============================================================================

import sys
import json
import uuid
from data_extract.shared.utils import (
    get_drive_service,
    check_gcs_marking,
    upload_to_gcs,
    get_target_folder_name,
)
from data_extract.shared.extract_logic import (
    ARCHIVAL_BUCKET,
    get_target_folder_id,
    get_valid_files,
    process_extraction,
)


def orchestrate_extract(target_child_folder: str) -> int:
    """
    Main orchestrator for the Google Drive to GCS ingestion lifecycle.

    Contract:
    - Resolves the specific target folder ID within the 'PARENT_FOLDER' hierarchy.
    - Enforces a deduplication check via GCS success markers to prevent redundant extraction.
    - Iteratively processes file extraction, archival, and pipeline mirroring.

    Invariants:
    - Atomicity: A run is marked as [SUCCESS] only if every file in the target
      folder is successfully processed and mirrored.
    - Idempotency: Uses GCS marking paths to skip previously completed folders.
    - Lineage: Generates a unique 'execution_id' (UUID4) for each orchestration attempt.

    Failures:
    - Returns 1 if any file extraction fails, the target folder is missing,
      or the handshake is invalid.
    - Returns 0 on successful completion or if a deduplication skip is triggered.
    """

    service = get_drive_service()
    metadata_path = f"logs/{target_child_folder}_metadata.json"
    archival_marking_path = f"status/{target_child_folder}.success"

    metadata = {
        "execution_id": str(uuid.uuid4()),
        "files_processed": [],
        "errors": [],
        "status": "success",
    }

    # Deduplication Check
    if check_gcs_marking(ARCHIVAL_BUCKET, archival_marking_path):
        print(f"[INFO]: {target_child_folder} already processed.")
        return 0

    # Extract target folder id
    folder_id = get_target_folder_id(target_child_folder, service)
    if not folder_id:
        return 1

    # Extract files
    files_in_drive = get_valid_files(folder_id, target_child_folder, service)

    # Exit if handshake failed or empty list
    if files_in_drive is None or len(files_in_drive) == 0:
        return 1

    for file in files_in_drive:

        archival_path = f"archive/{target_child_folder}/{file['name']}"
        pipeline_raw_path = f"raw/{file['name']}"

        ok, details = process_extraction(
            file, service, archival_path, pipeline_raw_path
        )

        if ok:
            metadata["files_processed"].append(details)
        else:
            metadata["errors"].append(details)
            metadata["status"] = "failed"

            # Upload failure metadata
            upload_to_gcs(ARCHIVAL_BUCKET, metadata_path, json.dumps(metadata))
            return 1

    # Upload success metadata and marker
    upload_to_gcs(ARCHIVAL_BUCKET, metadata_path, json.dumps(metadata))
    upload_to_gcs(ARCHIVAL_BUCKET, archival_marking_path, "")

    print(f"[SUCCESS]: Folder '{target_child_folder}' completely processed.")
    return 0


def main():
    target_folder = get_target_folder_name("operations")
    print(f"[INFO]: Starting extraction for folder: {target_folder}")

    exit_code = orchestrate_extract(target_folder)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

# =============================================================================
# END OF SCRIPT
# =============================================================================
