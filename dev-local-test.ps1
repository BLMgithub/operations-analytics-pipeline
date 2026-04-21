param(
    [string]$Buildtag = "",
    [string]$Testname = "",
    [string]$Memory = "",
    [string]$Memswap = "",
    [string]$Cpu = "",
    [string]$Threads = "",
    [string]$Data = ""
)

$ErrorActionPreference = 'Stop'

Write-Host ""
Write-Host "BUILDING IMAGE LOCALLY" -ForegroundColor Blue

docker build --no-cache -t $Buildtag -f data_pipeline/Dockerfile .

Write-Host ""
Write-Host "MOUNTING LOCAL DATA DIRECTORY AND RUN TEST" -ForegroundColor Blue

docker run --rm `
  --name $Testname `
  -v "${Data}:/app/data" `
  --memory=$Memory `
  --memory-swap=$Memswap `
  --cpus=$Cpu `
  -e POLARS_MAX_THREADS=$Threads `
  $Buildtag