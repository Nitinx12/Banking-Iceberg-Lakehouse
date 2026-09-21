@echo off
REM Banking Data Platform - tasks.bat mirror of Makefile (Architecture 16.1)
REM Heavy data tasks (Bronze/Silver/Gold) run Scala via sbt (Architecture 7.1: JVM shuffle
REM for 2M+ row tables). Quick monitoring/ops tasks run shell/PowerShell wrappers.
REM Usage: tasks.bat help | up | health | status | monitor | ingest | ingest_py | silver | ...
setlocal EnableDelayedExpansion

if "%~1"=="" goto help
set CMD=%~1
set ARG2=%~2

if /I "%CMD%"=="help" goto help
if /I "%CMD%"=="env" goto env
if /I "%CMD%"=="setup" goto setup
if /I "%CMD%"=="hooks" goto hooks
if /I "%CMD%"=="up" goto up
if /I "%CMD%"=="down" goto down
if /I "%CMD%"=="health" goto health
if /I "%CMD%"=="status" goto status
if /I "%CMD%"=="monitor" goto monitor
if /I "%CMD%"=="ingest" goto ingest
if /I "%CMD%"=="ingest_py" goto ingest_py
if /I "%CMD%"=="silver" goto silver
if /I "%CMD%"=="silver_scala" goto silver_scala
if /I "%CMD%"=="gold" goto gold
if /I "%CMD%"=="publish" goto publish
if /I "%CMD%"=="dbt_build" goto dbt_build
if /I "%CMD%"=="dq" goto dq
if /I "%CMD%"=="dashboard" goto dashboard
if /I "%CMD%"=="tf_plan" goto tf_plan
if /I "%CMD%"=="tf_apply" goto tf_apply
if /I "%CMD%"=="seed_mongo" goto seed_mongo
if /I "%CMD%"=="docs" goto docs
if /I "%CMD%"=="jars" goto jars
if /I "%CMD%"=="clean" goto clean
echo Unknown target: %CMD%
goto help

:help
echo Banking Data Platform - tasks.bat targets
echo.
echo HEAVY (Scala via sbt - Architecture 7.1):
echo   tasks.bat ingest        - Bronze ingestion (sbt, Phase 7 build.sbt required)
echo   tasks.bat ingest_py     - Bronze via Python fallback (uv, no sbt needed)
echo   tasks.bat silver        - Silver heavy tables (Scala: customers/transactions/card_txns)
echo   tasks.bat gold          - Gold star-schema build (Scala)
echo.
echo QUICK MONITORING (shell/PowerShell - scripts/sh and scripts/ps1):
echo   tasks.bat health        - service healthcheck (mongo/postgres/minio/airflow)
echo   tasks.bat status        - pipeline run status + watermarks from ops tables
echo   tasks.bat monitor       - dbt run_results.json report + Pushgateway metrics
echo   tasks.bat dq            - DQ checks + dbt test (fail-closed gate)
echo.
echo PIPELINE (other):
echo   tasks.bat publish       - publish Silver/Gold to Postgres serving
echo   tasks.bat dbt_build     - dbt build (scripts\ps1\run_dbt.ps1)
echo.
echo INFRA / DEV:
echo   tasks.bat env           - create .env from .env.example if missing
echo   tasks.bat setup         - uv sync + install hooks
echo   tasks.bat hooks         - set core.hooksPath to .githooks
echo   tasks.bat up [PROFILE]  - docker compose --profile ^<PROFILE^> up -d  (default core)
echo   tasks.bat down          - compose down
echo   tasks.bat lint          - ruff check + format
echo   tasks.bat test          - pytest -q
echo   tasks.bat test_fast     - pytest -q -m "not slow"
echo   tasks.bat seed_mongo    - load sample dataset into local Mongo
echo   tasks.bat jars          - download pinned Spark/Iceberg JARs to jars/ (stable offline)
echo   tasks.bat clean         - remove caches
goto :eof

:env
if not exist .env (
  copy /Y .env.example .env >nul
  echo created .env from .env.example - EDIT secrets
) else (
  echo .env exists
)
goto :eof

:setup
call :env
where uv >nul 2>&1
if %ERRORLEVEL%==0 (
  uv sync --group dev --group ingestion --group transform --group quality --group dashboard
) else (
  echo uv not found - install from https://docs.astral.sh/uv/ or use pip
)
git config core.hooksPath .githooks
echo setup done - run: tasks.bat up core
goto :eof

:hooks
git config core.hooksPath .githooks
echo hooks enabled (.githooks)
goto :eof

:up
set PROFILE=%ARG2%
if "%PROFILE%"=="" set PROFILE=core
docker compose --profile %PROFILE% up -d
echo up --profile %PROFILE% done; check: tasks.bat health
goto :eof

:down
docker compose down
goto :eof

REM ---------------------------------------------------------------- quick monitoring (shell)

:health
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\ps1\healthcheck.ps1
goto :eof

:status
echo == ops.pipeline_runs (latest 5) ==
docker exec banking_postgres psql -U postgres -d banking_dw -c "select run_id, stage, status, rows_written, finished_at from ops.pipeline_runs order by started_at desc limit 5" 2>nul
if %ERRORLEVEL% NEQ 0 echo postgres not reachable - tasks.bat up first
echo == watermarks ==
docker exec banking_postgres psql -U postgres -d banking_dw -c "select collection, watermark_value, updated_at from ops.watermarks order by collection" 2>nul
goto :eof

:monitor
if exist dbt\banking_dbt\target\run_results.json (
  scala-cli run dbt\monitor\dbt-report.scala -- dbt\banking_dbt\target\run_results.json
) else (
  echo no run_results.json yet - run tasks.bat dbt_build first
)
goto :eof

REM ---------------------------------------------------------------- heavy tasks (Scala)

:ingest
echo Bronze ingestion via Scala - requires sbt + jobs\transform\scala\build.sbt (Phase 7)
sbt ";project jobs.transform.scala; runMain jobs.ingestion.scala.BronzeIngestion"
goto :eof

:ingest_py
echo Bronze via Python fallback - proven CE path, same idempotency contract
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\ps1\run_ingestion.ps1 %*
goto :eof

:silver
echo Silver (3 heavy tables) via Scala - requires sbt + build.sbt (Phase 7)
sbt ";project jobs.transform.scala; runMain jobs.transform.scala.SilverAll"
goto :eof

:silver_scala
echo Single-table Silver via Scala - usage: tasks.bat silver_scala SilverTransactions
sbt ";project jobs.transform.scala; runMain jobs.transform.scala.%ARG2%"
goto :eof

:gold
echo Gold build via Scala - requires sbt + build.sbt (Phase 7)
sbt ";project jobs.transform.scala; runMain jobs.transform.scala.GoldBuild"
goto :eof

REM ---------------------------------------------------------------- other pipeline tasks

:publish
where uv >nul 2>&1
if %ERRORLEVEL%==0 (
  uv run python -m jobs.publish.serving --table fct_transactions --run-id tasks-publish
  uv run python -m jobs.publish.serving --table dim_customer --run-id tasks-publish
) else (
  python -m jobs.publish.serving --table fct_transactions --run-id tasks-publish
  python -m jobs.publish.serving --table dim_customer --run-id tasks-publish
)
goto :eof

:dbt_build
echo dbt build (scripts\ps1\run_dbt.ps1)
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run dbt build --project-dir dbt\banking_dbt ) else ( dbt build --project-dir dbt\banking_dbt )
goto :eof

:dq
echo DQ checks + dbt test (scripts\ps1\run_dq.ps1)
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run python -m jobs.quality.checks ) else ( python -m jobs.quality.checks )
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run dbt test --project-dir dbt\banking_dbt ) else ( dbt test --project-dir dbt\banking_dbt )
goto :eof

:dashboard
echo Streamlit dashboard on http://localhost:8501
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run streamlit run dashboard\Home.py ) else ( streamlit run dashboard\Home.py )
goto :eof

:tf_plan
set TFENV=%ARG2%
if "%TFENV%"=="" set TFENV=dev
terraform -chdir=terraform\envs\%TFENV% init -backend-config=backend.hcl -reconfigure -input=false
terraform -chdir=terraform\envs\%TFENV% plan -input=false
goto :eof

:tf_apply
set TFENV=%ARG2%
if "%TFENV%"=="" set TFENV=dev
terraform -chdir=terraform\envs\%TFENV% init -backend-config=backend.hcl -reconfigure -input=false
terraform -chdir=terraform\envs\%TFENV% apply -input=false
goto :eof

:seed_mongo
echo Seeding Mongo - requires compose core up
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run python scripts\seed_mongo.py ) else ( python scripts\seed_mongo.py )
goto :eof

:docs
echo dbt docs + Data Docs
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run dbt docs generate --project-dir dbt\banking_dbt --target-path docs_site ) else ( dbt docs generate --project-dir dbt\banking_dbt --target-path docs_site )
echo dbt docs at dbt\banking_dbt\docs_site\index.html
goto :eof

:jars
echo Downloading pinned Spark/Iceberg JARs to jars/ ...
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\ps1\Download-Jars.ps1
goto :eof

:clean
for /d /r %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d" 2>nul
if exist .pytest_cache rd /s /q .pytest_cache 2>nul
if exist .ruff_cache rd /s /q .ruff_cache 2>nul
if exist .spark rd /s /q .spark 2>nul
if exist spark-warehouse rd /s /q spark-warehouse 2>nul
echo cleaned
goto :eof
