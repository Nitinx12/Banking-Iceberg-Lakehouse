# scripts/ps1/init_buckets.ps1 — MinIO bucket init
param([switch]$DryRun)
$ErrorActionPreference="Stop"
$logDir="logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
function log($m){ "$(Get-Date -Format o) [buckets] $m" | Tee-Object -FilePath "$logDir/buckets.log" -Append | Write-Host }
$ep=$env:S3_ENDPOINT; if(!$ep){$ep="http://minio:9000"}; $b=$env:S3_BUCKET; if(!$b){$b="banking-lakehouse"}
if($DryRun){ log "[dry-run] would mc mb local/$b at $ep"; exit 0 }
if(!$env:AWS_ACCESS_KEY_ID -or !$env:AWS_SECRET_ACCESS_KEY){ Write-Error "AWS_ACCESS_KEY_ID/SECRET not set"; exit 1 }
log "init bucket $b at $ep"
mc alias set local $ep $env:AWS_ACCESS_KEY_ID $env:AWS_SECRET_ACCESS_KEY
mc mb --ignore-existing "local/$b"
log "bucket $b ready"
