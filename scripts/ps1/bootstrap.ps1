# scripts/ps1/bootstrap.ps1 — Windows mirror of scripts/sh/bootstrap.sh
param([switch]$DryRun)
$ErrorActionPreference="Stop"
$logDir="logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
function log($m){ $ts=(Get-Date -Format o); "$ts [bootstrap] $m" | Tee-Object -FilePath "$logDir/bootstrap.log" -Append | Write-Host }
if(!(Test-Path ".env")){ if($DryRun){ log "[dry-run] would copy .env.example -> .env"} else { Copy-Item .env.example .env; log "created .env from .env.example" } } else { log ".env exists" }
if($DryRun){ log "[dry-run] would: uv sync --group dev && git config core.hooksPath .githooks" } else {
  try { uv sync --group dev } catch { log "uv sync failed — install uv" }
  git config core.hooksPath .githooks; log "hooks enabled"
}
log "bootstrap done — next: tasks.bat up core"
