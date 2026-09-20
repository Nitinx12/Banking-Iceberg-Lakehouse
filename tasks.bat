@echo off
REM Banking Data Platform — tasks.bat mirror of Makefile (Architecture 16.1)
REM Usage: tasks.bat help | env | setup | hooks | up core | down | lint | test | test_fast | seed_mongo | clean
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
if /I "%CMD%"=="lint" goto lint
if /I "%CMD%"=="test" goto test
if /I "%CMD%"=="test_fast" goto test_fast
if /I "%CMD%"=="ingest" goto ingest
if /I "%CMD%"=="dbt_build" goto dbt_build
if /I "%CMD%"=="dq" goto dq
if /I "%CMD%"=="dashboard" goto dashboard
if /I "%CMD%"=="tf_plan" goto tf_plan
if /I "%CMD%"=="tf_apply" goto tf_apply
if /I "%CMD%"=="seed_mongo" goto seed_mongo
if /I "%CMD%"=="docs" goto docs
if /I "%CMD%"=="clean" goto clean
echo Unknown target: %CMD%
goto help

:help
echo Banking Data Platform — tasks.bat targets
echo   tasks.bat help              - this list
echo   tasks.bat env               - create .env from .env.example if missing
echo   tasks.bat setup             - uv sync + install hooks
echo   tasks.bat hooks             - set core.hooksPath to .githooks
echo   tasks.bat up [PROFILE]      - docker compose --profile ^<PROFILE^> up -d  (default core)
echo   tasks.bat down              - compose down
echo   tasks.bat lint              - ruff check + format
echo   tasks.bat test              - pytest -q
echo   tasks.bat test_fast         - pytest -q -m "not slow"
echo   tasks.bat seed_mongo        - load sample dataset into local Mongo
echo   tasks.bat clean             - remove caches
goto :eof

:env
if not exist .env (
  copy /Y .env.example .env >nul
  echo created .env from .env.example — EDIT secrets
) else (
  echo .env exists
)
goto :eof

:setup
call :env
where uv >nul 2>&1
if %ERRORLEVEL%==0 (
  uv sync --group dev --group ingestion --group dashboard
) else (
  echo uv not found — install from https://docs.astral.sh/uv/ or use pip
)
git config core.hooksPath .githooks
echo setup done — run: tasks.bat up core
goto :eof

:hooks
git config core.hooksPath .githooks
echo hooks enabled (.githooks)
goto :eof

:up
set PROFILE=%ARG2%
if "%PROFILE%"=="" set PROFILE=core
docker compose --profile %PROFILE% up -d
echo up --profile %PROFILE% done; check: docker compose ps
goto :eof

:down
docker compose down
goto :eof

:lint
where uv >nul 2>&1
if %ERRORLEVEL%==0 (
  uv run ruff check .
  uv run ruff format --check .
) else (
  ruff check .
  ruff format --check .
)
goto :eof

:test
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run pytest -q ) else ( pytest -q )
goto :eof

:test_fast
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run pytest -q -m "not slow" ) else ( pytest -q -m "not slow" )
goto :eof

:ingest
echo Bronze ingestion — requires compose core up (scripts\ps1\run_ingestion.ps1)
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run python -m jobs.ingestion.bronze --all ) else ( python -m jobs.ingestion.bronze --all )
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
echo Seeding Mongo — requires compose core up and dataset in data/
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run python scripts\seed_mongo.py ) else ( python scripts\seed_mongo.py )
goto :eof

:docs
echo dbt docs + Data Docs
where uv >nul 2>&1
if %ERRORLEVEL%==0 ( uv run dbt docs generate --project-dir dbt\banking_dbt --target-path docs_site ) else ( dbt docs generate --project-dir dbt\banking_dbt --target-path docs_site )
echo dbt docs at dbt\banking_dbt\docs_site\index.html
goto :eof

:clean
for /d /r %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d" 2>nul
if exist .pytest_cache rd /s /q .pytest_cache 2>nul
if exist .ruff_cache rd /s /q .ruff_cache 2>nul
if exist .spark rd /s /q .spark 2>nul
if exist spark-warehouse rd /s /q spark-warehouse 2>nul
echo cleaned
goto :eof
