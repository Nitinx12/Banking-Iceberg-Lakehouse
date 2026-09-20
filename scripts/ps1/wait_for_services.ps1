# scripts/ps1/wait_for_services.ps1 — poll compose health
param([int]$Retries=30,[int]$SleepS=5)
$ErrorActionPreference="Stop"
$logDir="logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
function log($m){ "$(Get-Date -Format o) [wait] $m" | Tee-Object -FilePath "$logDir/wait.log" -Append | Write-Host }
for($i=1;$i -le $Retries;$i++){
  try { $r=docker compose --profile core ps 2>$null; if($r -match "healthy" -or $r -match "Up"){ log "core services healthy (attempt $i)"; exit 0 } } catch {}
  log "not ready — attempt $i/$Retries sleeping ${SleepS}s"; Start-Sleep -Seconds $SleepS
}
Write-Error "services not healthy after $Retries attempts — check: docker compose --profile core ps"; exit 1
