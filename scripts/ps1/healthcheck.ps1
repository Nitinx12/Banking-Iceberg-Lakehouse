# scripts/ps1/healthcheck.ps1 — service + pipeline freshness
$ErrorActionPreference="Continue"
$logDir="logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
function log($m){ "$(Get-Date -Format o) [healthcheck] $m" | Tee-Object -FilePath "$logDir/healthcheck.log" -Append | Write-Host }
$fail=0
function check($name,$sb){ try{ & $sb | Out-Null; log "✓ $name" } catch { Write-Host "✗ $name" -ForegroundColor Red; $script:fail=1 } }
log "healthcheck start"
try{ pg_isready -h $env:POSTGRES_HOST -p $env:POSTGRES_PORT -U $env:POSTGRES_USER | Out-Null; log "✓ postgres" } catch { log "✗ postgres"; $fail=1 }
try{ curl.exe -f http://localhost:9000/minio/health/live 2>$null | Out-Null; log "✓ minio" } catch { try{ curl.exe -f "$env:S3_ENDPOINT/minio/health/live" 2>$null | Out-Null; log "✓ minio" } catch { log "✗ minio"; $fail=1 } }
if($fail -eq 0){ log "healthcheck: all ok"; exit 0 } else { log "healthcheck: some checks failed"; exit 1 }
