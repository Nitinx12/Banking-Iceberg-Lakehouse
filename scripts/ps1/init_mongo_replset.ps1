# scripts/ps1/init_mongo_replset.ps1 — idempotent rs0 init (Windows)
param([switch]$DryRun)
$ErrorActionPreference="Stop"
$logDir="logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
function log($m){ "$(Get-Date -Format o) [mongo_init] $m" | Tee-Object -FilePath "$logDir/mongo_init.log" -Append | Write-Host }
if($DryRun){ log "[dry-run] would rs.initiate rs0"; exit 0 }
$u=$env:MONGO_INITDB_ROOT_USERNAME; $p=$env:MONGO_INITDB_ROOT_PASSWORD; $h=$env:MONGO_HOST; if(!$h){$h="mongo"}; $port=$env:MONGO_PORT; if(!$port){$port="27017"}
if(!$u -or !$p){ Write-Error "MONGO_INITDB_ROOT_USERNAME/PASSWORD not set"; exit 1 }
log "init replicaSet rs0 on ${h}:${port}"
mongosh --host "${h}:${port}" -u $u -p $p --authenticationDatabase admin --eval 'try{rs.status();print("rs already initiated")}catch(e){rs.initiate({_id:"rs0",members:[{_id:0,host:"mongo:27017"}]});print("rs initiated")}'
for($i=0;$i -lt 15;$i++){
  $ok=mongosh --host "${h}:${port}" -u $u -p $p --authenticationDatabase admin --quiet --eval 'rs.status().ok' 2>$null
  if($ok -match "1"){ log "mongo RS ready"; exit 0 }; Start-Sleep -Seconds 2
}
Write-Error "mongo RS init timeout"; exit 1
