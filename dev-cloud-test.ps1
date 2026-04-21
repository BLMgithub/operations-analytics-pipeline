param(
  [string]$ProjectId = "",
  [string]$ArtifactReg = "",
  [string]$GcpDocker = "",
  [string]$ImageTag = "",
  [string]$Region = "",
  [string]$BqDatasetId = "",
  [string]$Memory = "",
  [string]$Cpu = "",
  [string]$Threads = ""
)

$ErrorActionPreference = 'Stop'

$IMAGE_PATH = "$GcpDocker/$ProjectId/$ArtifactReg/$ImageTag"

Write-Host ""
Write-Host "BUILDING IMAGE LOCALLY" -ForegroundColor Blue

docker build --no-cache -t $IMAGE_PATH -f data_pipeline/Dockerfile .

Write-Host ""
Write-Host "PUSHING IMAGE TO CLOUD REPO" -ForegroundColor Blue

docker push $IMAGE_PATH

Write-Host ""
Write-Host "UPDATING JOB" -ForegroundColor Blue

gcloud run jobs update operations-pipeline-dev `
  --image $IMAGE_PATH `
  --update-env-vars GCP_PROJECT=$ProjectId `
  --update-env-vars BQ_DATASET_ID=$BqDatasetId `
  --update-env-vars POLARS_MAX_THREADS=$Threads `
  --region $Region `
  --memory $Memory `
  --cpu $Cpu

Write-Host ""
Write-Host "EXECUTING CLOUD JOB" -ForegroundColor Blue

gcloud run jobs execute operations-pipeline-dev --region $Region