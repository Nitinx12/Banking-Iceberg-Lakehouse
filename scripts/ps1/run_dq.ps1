# scripts/ps1/run_dq.ps1 — GX + dbt tests Windows mirror
param([switch]$DryRun)
$ErrorActionPreference="Stop"
$logDir="logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
function log($m){ "$(Get-Date -Format o) [dq] $m" | Tee-Object -FilePath "$logDir/dq.log" -Append | Write-Host }
if(!(Test-Path "gx/great_expectations.yml")){ Write-Error "GX config not found"; exit 1 }
if($DryRun){ log "[dry-run] would: uv run python -m jobs.quality.checks + dbt test"; exit 0 }
log "dq checks start"
uv run python -m jobs.quality.checks
if(Test-Path "dbt/banking_dbt/dbt_project.yml"){ try{ uv run dbt test --project-dir dbt/banking_dbt } catch { dbt test --project-dir dbt/banking_dbt } }
log "dq checks done"
