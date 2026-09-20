# scripts/ps1/run_dbt.ps1 — dbt build Windows mirror
param([string]$Selector="all",[switch]$DryRun)
$ErrorActionPreference="Stop"
$logDir="logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
function log($m){ "$(Get-Date -Format o) [dbt] $m" | Tee-Object -FilePath "$logDir/dbt.log" -Append | Write-Host }
$dir="dbt/banking_dbt"
if(!(Test-Path "$dir/dbt_project.yml")){ Write-Error "dbt project not found at $dir"; exit 1 }
if($DryRun){ log "[dry-run] would: dbt build --project-dir $dir --select $Selector"; exit 0 }
log "dbt build selector=$Selector"
try { uv run dbt build --project-dir $dir --select $Selector } catch { dbt build --project-dir $dir --select $Selector }
log "dbt build done"
