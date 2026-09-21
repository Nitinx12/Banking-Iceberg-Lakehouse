# scripts/ps1/Download-Jars.ps1 — download pinned Spark/Iceberg JARs for stable offline runs (Windows)
# Pinned to match jobs/common/spark.py:70 + jobs/transform/scala/build.sbt
param([switch]$DryRun)
$ErrorActionPreference = "Stop"
$JarsDir = "jars"
New-Item -ItemType Directory -Force -Path $JarsDir | Out-Null
$Base = "https://repo1.maven.org/maven2"
$Jars = @{
  "iceberg-spark-runtime-3.5_2.12-1.5.2.jar" = "$Base/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
  "postgresql-42.7.4.jar"                   = "$Base/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
  "hadoop-aws-3.3.4.jar"                    = "$Base/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  "aws-java-sdk-bundle-1.12.780.jar"        = "$Base/com/amazonaws/aws-java-sdk-bundle/1.12.780/aws-java-sdk-bundle-1.12.780.jar"
}
foreach ($name in $Jars.Keys) {
  $url = $Jars[$name]; $dest = Join-Path $JarsDir $name
  if (Test-Path $dest) { Write-Host "exists $dest — skip"; continue }
  if ($DryRun) { Write-Host "would download $url -> $dest"; continue }
  Write-Host "fetch $url"
  Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing
  Write-Host "saved $dest ($((Get-Item $dest).Length / 1MB) MB)"
}
if (-not $DryRun) {
  if ((Get-Command sbt -ErrorAction SilentlyContinue) -and (Test-Path "jobs/transform/scala/build.sbt")) {
    Write-Host "warming Ivy via sbt update..."
    Push-Location jobs/transform/scala; try { sbt update } catch { Write-Warning "sbt update failed: $_" } finally { Pop-Location }
  }
  Write-Host "done — jars in ${JarsDir}: $((Get-ChildItem $JarsDir/*.jar -ErrorAction SilentlyContinue | Measure-Object).Count) files"
  Get-ChildItem $JarsDir -ErrorAction SilentlyContinue | Format-Table Name, Length -AutoSize
  Write-Host "use: `$env:SPARK_JARS='jars/*.jar'; uv run python -m jobs.ingestion.bronze --all  — auto-picks jars/*.jar if present"
}
