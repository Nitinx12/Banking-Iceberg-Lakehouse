# scripts/ps1/lib.ps1 — shared helpers for all PowerShell scripts (Architecture 16.2)
# Mirrors scripts/sh/lib.sh — source with: . "$PSScriptRoot/lib.ps1"
$ErrorActionPreference = "Stop"
$global:LogDir = if ($env:LOG_DIR) { $env:LOG_DIR } else { "logs" }
$global:Stage  = if ($env:STAGE)  { $env:STAGE }  else { "generic" }
New-Item -ItemType Directory -Force -Path $global:LogDir | Out-Null
$global:LogFile = Join-Path $global:LogDir "$($global:Stage).log"

function Write-Log($msg)  { $line="[$(Get-Date -Format o)] [$($global:Stage)] $msg"; $line | Tee-Object -FilePath $global:LogFile -Append | Write-Host }
function Write-Warn($msg) { $line="[$(Get-Date -Format o)] [$($global:Stage)] WARN: $msg"; $line | Tee-Object -FilePath $global:LogFile -Append | Write-Host -ForegroundColor Yellow }
function Die($msg)        { Write-Warn "ERROR: $msg"; throw $msg }

function Require-Env($name) {
  if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name))) { Die "required env $name is not set" }
}
function Require-Cmd($name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) { Die "required command $name not found" }
}

# Load .env if present (simple KEY=VALUE, no export)
if (Test-Path ".env") {
  Get-Content ".env" | ForEach-Object {
    if ($_ -match '^\s*#' -or $_ -notmatch '=') { return }
    $k,$v = $_.Split('=',2); $k=$k.Trim(); $v=$v.Trim().Trim('"').Trim("'")
    if ($k -and -not [Environment]::GetEnvironmentVariable($k)) { Set-Item -Path "env:$k" -Value $v }
  }
}
