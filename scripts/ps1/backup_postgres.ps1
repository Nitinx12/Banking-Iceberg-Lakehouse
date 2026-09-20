# scripts/ps1/backup_postgres.ps1 — pg_dump with rotation (Windows)
param([switch]$DryRun,[string]$BackupDir="backups",[int]$Retention=7)
$ErrorActionPreference="Stop"
$logDir="logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
function log($m){ "$(Get-Date -Format o) [backup] $m" | Tee-Object -FilePath "$logDir/backup.log" -Append | Write-Host }
$hostName=$env:POSTGRES_HOST; if(!$hostName){$hostName="postgres"}; $port=$env:POSTGRES_PORT; if(!$port){$port="5432"}; $db=$env:POSTGRES_WAREHOUSE_DB; if(!$db){$db="banking_dw"}; $user=$env:POSTGRES_USER; if(!$user){$user="postgres"}
if(!$db -or !$user){ Write-Error "POSTGRES_* not set"; exit 1 }
New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null
$ts=Get-Date -Format "yyyyMMdd-HHmmss"; $file="$BackupDir/${db}-${ts}.sql.gz"
if($DryRun){ log "[dry-run] would: pg_dump -h $hostName -p $port -U $user $db | gzip > $file"; exit 0 }
log "backup $db -> $file"
$env:PGPASSWORD=$env:POSTGRES_PASSWORD; pg_dump -h $hostName -p $port -U $user $db | gzip > $file
Get-ChildItem "$BackupDir/${db}-*.sql.gz" | Sort-Object LastWriteTime -Descending | Select-Object -Skip $Retention | Remove-Item -Force -ErrorAction SilentlyContinue
log "backup done: $file"
