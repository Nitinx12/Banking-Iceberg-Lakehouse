# scripts/ps1/run_ingestion.ps1 — batch Bronze ingestion Windows mirror
param([string]$BatchId="",[string]$RunId="",[switch]$DryRun)
$ErrorActionPreference="Stop"
$logDir="logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
function log($m){ "$(Get-Date -Format o) [ingest] $m" | Tee-Object -FilePath "$logDir/ingest.log" -Append | Write-Host }
$args=@("--all")
if($BatchId){$args+=@("--batch-id",$BatchId)}
if($RunId){$args+=@("--run-id",$RunId)}
if($DryRun){$args+=@("--dry-run"); log "[dry-run] would: uv run python -m jobs.ingestion.bronze $($args -join ' ')"; exit 0}
log "run_ingestion start $($args -join ' ')"
uv run python -m jobs.ingestion.bronze @args
log "ingestion done"
