# =============================================================================
# Google Cloud Storage path adapter
# =============================================================================

from data_pipeline.shared.run_context import RunContext
from pathlib import Path
from google.cloud import storage
import shutil


def _split_gcs_path(path: str):
    """
    Internal helper for GCS path parsing.

    Contract:
    - Converts a 'gs://bucket/prefix' string into a (bucket, prefix) tuple.
    """

    path = path.replace("gs://", "")
    bucket, *rest = path.split("/", 1)
    prefix = rest[0] if rest else ""

    return bucket, prefix


def download_raw_snapshot(run_context: RunContext) -> None:
    """
    Synchronizes the raw data snapshot from Cloud Storage to the local workspace.

    Contract:
    - Downloads files from the 'storage_raw_path' to the local 'raw_snapshot_path'.

    Side Effects:
    - Reconstructs the source directory structure on the local filesystem.
    """

    source = run_context.storage_raw_path
    destination = run_context.raw_snapshot_path

    # Local filesystem case
    if not str(source).startswith("gs://"):
        shutil.copytree(source, destination, dirs_exist_ok=True)
        return

    # GCS case
    client = storage.Client()

    bucket_name, prefix = _split_gcs_path(source)

    bucket = client.bucket(bucket_name)

    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith("/"):
            continue

        target = destination / Path(blob.name).name
        target.parent.mkdir(parents=True, exist_ok=True)

        blob.download_to_filename(target)


def upload_publish_artifacts(run_context: RunContext) -> None:
    """
    Promotes validated Semantic artifacts to the versioned production zone.

    Contract:
    - Synchronizes the local 'semantic/' directory to the versioned 'published/v{run_id}' path.
    - Purpose: Archives final analytical modules for production activation.
    """

    source = run_context.semantic_path
    destination = run_context.version_path

    # Local filesystem case
    if not str(destination).startswith("gs://"):
        shutil.copytree(source, destination)
        return

    # GCS case
    client = storage.Client()

    bucket_name, prefix = _split_gcs_path(destination)

    bucket = client.bucket(bucket_name)

    for file in source.rglob("*"):
        if file.is_file():
            blob = bucket.blob(f"{prefix}/{file.relative_to(source)}")
            blob.upload_from_filename(file)


def upload_run_artifacts(run_context: RunContext) -> None:
    """
    Uploads execution telemetry (logs and metadata) to the run artifact zone.

    Contract:
    - Persists 'metadata.json' and the 'logs/' directory to 'storage_runs_path'.
    - Purpose: Provides visibility and traceability for both successful and failed runs.
    """

    destination = run_context.storage_runs_path
    logs_path = run_context.logs_path
    metadata_path = run_context.metadata_path

    # Local storage case
    if not str(destination).startswith("gs://"):

        target = Path(destination)
        target.mkdir(parents=True, exist_ok=True)

        if metadata_path.exists():
            shutil.copy2(metadata_path, target / "metadata.json")

        if logs_path.exists():
            shutil.copytree(logs_path, target / "logs", dirs_exist_ok=True)

        return

    # GCS storage case
    client = storage.Client()

    bucket_name, prefix = _split_gcs_path(destination)
    bucket = client.bucket(bucket_name)

    # Upload metadata
    if metadata_path.exists():
        blob = bucket.blob(f"{prefix}/metadata.json")
        blob.upload_from_filename(metadata_path)

    # Upload logs
    if logs_path.exists():
        for file in logs_path.rglob("*"):
            if file.is_file():
                blob = bucket.blob(f"{prefix}/logs/{file.relative_to(logs_path)}")
                blob.upload_from_filename(file)


def upload_contracted_directory(run_context: RunContext) -> None:
    """
    Promotes local Silver-layer artifacts to the persistent Cloud Silver Storage.

    Contract:
    - Synchronizes the local 'contracted/' directory to 'storage_contracted_path'.
    - Excludes the 'id_mapping' directory to prevent cross-contamination.
    - Purpose: Archives newly cleaned data for delta accumulation and historical lineage.
    """

    source = run_context.contracted_path
    destination = run_context.storage_contracted_path

    # Local filesystem case
    if not str(destination).startswith("gs://"):

        Path(destination).mkdir(parents=True, exist_ok=True)

        for file in source.iterdir():
            if file.is_file():
                target_file = f"{destination}/{file.name}"

                shutil.copyfile(file, target_file)

        return

    # GCS case
    client = storage.Client()

    bucket_name, prefix = _split_gcs_path(destination)
    bucket = client.bucket(bucket_name)

    for file in source.rglob("*"):
        if file.is_file() and "id_mapping" not in file.parts:

            blob = bucket.blob(f"{prefix}/{file.relative_to(source)}")
            blob.upload_from_filename(file)


def download_contracted_datasets(run_context: RunContext) -> None:
    """
    Populate the reconstructed local contracted/ with full historical delta set from Silver Cloud storage.

    Contract:
    - Downloads the full accumulated Silver state from 'storage_contracted_path'.
    """

    source = run_context.storage_contracted_path
    destination = run_context.contracted_path

    # Local filesystem case
    if not str(source).startswith("gs://"):
        shutil.copytree(source, destination, dirs_exist_ok=True)
        return

    # GCS case
    client = storage.Client()

    bucket_name, prefix = _split_gcs_path(source)

    bucket = client.bucket(bucket_name)

    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith("/"):
            continue

        target = destination / Path(blob.name).name
        target.parent.mkdir(parents=True, exist_ok=True)

        blob.download_to_filename(target)


def promote_new_mapping_files(runtime_dir: Path, destination: Path | str) -> None:
    """
    Synchronizes new UUID mapping files from the local temporary directory to central storage.

    Contract:
    - Identifies all '*_mapping.parquet' files in the local 'runtime_dir'.
    - Promotes them to the persistent 'destination' (local directory or GCS bucket).
    """

    if not runtime_dir.exists():
        return

    destination_str = str(destination).replace("\\", "/")

    # Local filesystem case
    if not destination_str.startswith("gs://"):
        Path(destination).mkdir(parents=True, exist_ok=True)
        for file in runtime_dir.glob("*_mapping.parquet"):
            shutil.copy2(file, Path(destination))
        return

    # GCS case
    client = storage.Client()
    bucket_name, prefix = _split_gcs_path(destination_str)
    bucket = client.bucket(bucket_name)

    for file in runtime_dir.glob("*_mapping.parquet"):
        if file.is_file():
            # Create a blob with the target filename in the bucket
            blob = bucket.blob(f"{prefix}/{file.name}")
            blob.upload_from_filename(str(file))
