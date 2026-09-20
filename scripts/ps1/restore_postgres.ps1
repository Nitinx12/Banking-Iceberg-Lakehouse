# scripts/ps1/restore_postgres.ps1 — restore (destructive — requires -DryRun preview)
param([string]$File="",[switch]$DryRun)
$ErrorActionPreference="Stop"
$logDir="logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
function log($m){ "$(Get-Date -Format o) [restore] $m" | Tee-Object -FilePath "$logDir/restore.log" -Append | Write-Host }
if(!$File){ Write-Error "usage: restore_postgres.ps1 -File <backup.sql.gz> [-DryRun]"; exit 1 }
if(!(Test-Path $File)){ Write-Error "backup not found: $File"; exit 1 }
if($DryRun){ log "[dry-run] would: gunzip -c $File | psql ..."; log "[dry-run] aborting — destructive"; exit 0 }
$hostName=$env:POSTGRES_HOST; if(!$hostName){$hostName="postgres"}; $port=$env:POSTGRES_PORT; if(!$port){$port="5432"}; $db=$env:POSTGRES_WAREHOUSE_DB; $user=$env:POSTGRES_USER
Write-Host "Restore $File into $db? type YES to confirm: " -NoNewline; $ans=Read-Host; if($ans -ne "YES"){ Write-Error "restore cancelled"; exit 1 }
$env:PGPASSWORD=$env:POSTGRES_PASSWORD; gunzip -c $File | psql -h $hostName -p $port -U $user $db
log "restore done"
