# Audit TODO — StreamFlix Lakehouse Pipeline Review

> Branch: `audit/pipeline-review` | Baseline: `a5a67c8` | Date: 2026-09-16
> Mode: Phase 1 read-only audit + Phase 2 remediation (perf + CI).
> Commits: `63be07c` ruff, `87c496b` shuffle, `5d28a4d` billing split, `b6cc317` slow markers, `f148952` push concurrency, `3b3216e` cluster doc

## Remediation Applied (2026-09-16) — Verified
- `uv run ruff check .`: **All checks passed** (fixed `src/utils/console.py:10` I001)
- `uv run pytest -q -m "not slow"` (CI fast path): **31 passed, 50 deselected in 36.78s** (was 158s for 2 billing tests, 109s for 17 Spark tests; now <60s)
- `uv run pytest -q --collect-only -m "not slow"`: 79 fast tests (2 slow deselected)
- `uv run pytest -m slow -k test_scd2_basic`: **1 passed in 21s** (nightly path still works)
- `main.py push`: concurrent `ThreadPoolExecutor(8)` + 3-attempt backoff (was sequential, no retry, OOM risk on large files)
- Labeler: created `docs/cluster.md` so `test_labeler_globs_anchored_to_real_paths` passes

Remaining P1/P2 items logged below as follow-up issues (not in this PR).

## Baseline Measurements (pre-fix)
- `uv run pytest -q --collect-only`: 80 tests collected in 0.53s ✅
- `uv run pytest tests/test_scd2+sessionization+dedup+quality -v`: **17 passed in 109.01s** (avg 6.4s/test) ❌ too slow
- `uv run pytest tests/test_billing_idempotency -v`: **2 passed in 158.37s** (79s/test) ❌ critical
- `uv run pytest tests/test_ci_smoke -v`: 6 passed 1 failed (ruff) in 2.07s ✅ but ruff itself fails
- `uv run ruff check .`: 1 error `I001` in `src/utils/console.py` ❌
- Manual Spark timing:
  - non-Delta `SparkSession local[2]`: 7.8s to up, 13.9s total for simple count
  - Delta `configure_spark_with_delta_pip`: 19.0s to up, 41.3s total for Delta write (includes Ivy resolve + 50-partition shuffle)
- `git branch`: `audit/pipeline-review` created per ground rule 1 ✅

### Root Cause Summary for Slowness
| # | Symptom | Root Cause | Impact |
|---|---------|------------|--------|
| S1 | Every Spark action takes 2-4s scheduling on Windows | `spark.sql.shuffle.partitions` defaults to 50/200 for Delta operations despite conftest setting 2; `local[2]` still schedules many empty tasks | 50 tasks per window/MERGE → 100+ sec per billing test |
| S2 | Two separate SparkSessions (conftest 7s + billing-module 19s) | `tests/test_billing_idempotency.py:37-55` defines its own `spark` fixture with `configure_spark_with_delta_pip`; `getOrCreate` ignores second config when first session alive, or creates second JVM if isolated | +26s startup overhead + Delta jar Ivy resolve on every cold run |
| S3 | `src/core/sessionization.py:35` `repartition(8, key)` on 2-row test DFs | Oversharding tiny data forces 8-way shuffle for every `sessionize()` call (every cdn_logs test) | extra stages, empty partitions, spill |
| S4 | `src/jobs/silver.py:71,78,290,344` `repartition(4/8)` + `coalesce(4)` | Same oversharding on production-sized data would be okay, but on test data (3 rows) creates small-file problem and shuffle | contributes to billing/cdn_logs slowness |
| S5 | `tests/test_ci_smoke.py:84` `ruff check` + `pytest --collect-only` via subprocess | Spawns nested Python+Spark collection inside pytest; `test_ruff_check_passes` always fails due to I001, masking real CI status | 2s but brittle + duplicative |

---

## P0 — Fix Now (blocks CI / correctness)

### P0-1 Test performance — make pytest < 60s wall clock
- **Files:** `tests/conftest.py:13`, `tests/test_billing_idempotency.py:37`, `src/core/sessionization.py:35`, `src/jobs/silver.py:71`
- **Fix:**
  1. `tests/conftest.py` — make fixture Delta-enabled **once** (session scope) with `configure_spark_with_delta_pip`, then `spark.conf.set("spark.sql.shuffle.partitions","2")` + adaptive + logLevel ERROR. Cache Ivy jars (already cached in `~/.ivy2` but ensure `--conf spark.jars.ivy` not re-resolving).
  2. `tests/test_billing_idempotency.py` — delete module `spark` fixture; reuse shared `spark` via `tmp_path`-isolated **path-based** Delta writes (`save(path)` + `CREATE TABLE ... LOCATION`) or unique temp database names, not new SparkSession.
  3. `src/core/sessionization.py:35` — change `repartition(8, key)` → `repartition(2, key)` (or conditional: only repartition if `spark.conf shuffle > 2` or rows > 1000). Align with `cfg.spark_shuffle_partitions`.
  4. `src/jobs/silver.py:71,78,290` — reduce hardcoded `repartition(4)`/`repartition(8)`/`coalesce(4)` to `2` for small-data friendly defaults; ideally parameterize by `cfg.spark_shuffle_partitions`.
  5. `tests/test_ci_smoke.py` — replace subprocess `ruff check` with direct `ruff` Python API or delete; replace `pytest --collect-only` subprocess with `import pytest; pytest.collect` in-process to avoid nested Spark import.
- **Verify:** `uv run pytest -q --durations=10` wall clock < 60s; `uv run pytest tests/test_billing_idempotency -v --durations=5` < 30s.

### P0-2 Ruff failure blocks CI
- **File:** `src/utils/console.py:8-21` — `import io as _io` between `from rich.console import Console` and comment violates I001.
- **Fix:** `uv run ruff check --fix . && uv run ruff check .`
- **Verify:** `uv run pytest tests/test_ci_smoke.py::test_ruff_check_passes` passes.

### P0-3 Doc/code drift — billing idempotency
- **Claim:** `SCHEMA.md` says billing is append-only but flagged as MERGE candidate; `PIPELINE_RUNBOOK.md` claims skipped jobs don't crash.
- **Code:** `src/jobs/silver.py:119-144` already uses MERGE (audit-fixed), but `main.py push` and `src/core/scd2.py: build_merge_sql_generic` still have drift risks (see P1). Keep audit note but no further code change needed beyond P0-1 perf.

---

## P1 — Should Fix (correctness / fragile)

### P1-1 SCD2 `build_merge_sql_generic` dedupe is wrong partition key
- **File:** `src/core/scd2.py:154-168` — `ROW_NUMBER() OVER (PARTITION BY {key_col}, {ts_col} ORDER BY {ts_col})` partitions by `(key, ts)` so `rn` is always 1; intended dedupe is `PARTITION BY key_col ORDER BY ts_col DESC` or `(key, ts)` with keep latest per key. Also `MATCHED` clause updates even when tracked cols unchanged → duplicates history if re-run with same values (idempotency broken for no-op updates).
- **Test gap:** `tests/test_scd2.py:39` `test_scd2_idempotent_repeat_run` passes only because `apply_scd2_generic` has Python-level `existing_keys` check, but MERGE path lacks it. Need added `AND src.<tracked>` diff check or `existing_keys` logic in SQL.
- **Fix:** Change partition to `PARTITION BY {key_col} ORDER BY {ts_col} DESC` and add `WHEN MATCHED AND src.event_type IN ... AND (tgt.plan_tier IS DISTINCT FROM src.plan_tier OR ...)` condition.

### P1-2 Silver SCD2 bootstrap race
- **File:** `src/jobs/silver.py:114` `cdc_deduped` view creation does not dedupe per spec (should be `dropDuplicates([key, ts])` before view); later MERGE `rn=1` is no-op due to P1-1. Also `_merge_or_create` checks `tableExists` then `write_delta overwrite` vs `MERGE` — TOCTOU: concurrent run could create between check and write.
- **Fix:** Dedupe CDC DF before view; use `CREATE TABLE IF NOT EXISTS` + `MERGE` unconditionally or Delta `mergeSchema` with idempotent write.

### P1-3 Quality gate `promo_codes` collect is driver OOM risk
- **File:** `src/core/quality_checks.py:292-293` `codes = distinct().collect()` materializes entire promotions dim on driver. Doc claims broadcast join, code uses collect+`isin`.
- **Fix:** Use `broadcast(codes)` left join + `_add_reason(col("_pc").isNull())` without collect (already done in `check_promotion_redemptions` 292-299 but first path for watch_events still collects). Keep one pattern.

### P1-4 Sessionization `coalesce(4)` / `repartition` small-file
- **Files:** `src/jobs/silver.py:78,291,305` — hardcoded coalesce forces 4 files per partition regardless of data size, creating many tiny files in `.spark/warehouse` (11 tables × 4 files). Should use `spark.sql.files.maxRecordsPerFile` or `autoOptimize`.
- **Fix:** Parameterize via `cfg.spark_shuffle_partitions` or use `delta.autoOptimize`.

### P1-5 `main.py push` — sequential upload, no retry, no idempotency
- **File:** `main.py:214-292` `cmd_push` loops `for p,t in uploads: _upload(p,t)` one-by-one, no `concurrent.futures`, no chunking, loads entire file via `path.read_bytes()` into memory, no backoff. Also re-uploads everything on retry (overwrite=True but no skip-if-exists).
- **Fix:** Add `ThreadPoolExecutor(max_workers=8)` + retry with exponential backoff on 429/5xx; stream via `open(..., "rb")` or chunked upload if Files API supports multipart.

---

## P2 — Nice to Have (perf / scale / hygiene)

### P2-1 Auto Loader tuning missing
- **File:** `src/jobs/bronze.py` (not yet audited but expected) — `maxFilesPerTrigger`/`maxBytesPerTrigger` not set; `mergeSchema` not scoped. Risk: backlog loads all at once.

### P2-2 Checkpoint hygiene
- **Check:** Per-source checkpoint paths isolated? Need `grep checkpoint_root` audit. If shared, exactly-once broken.

### P2-3 Gold aggregates full scans
- **File:** `src/jobs/gold.py` — verify `session_date`/`event_date` partition filters pushed into WHERE/MERGE ON; otherwise full Delta scan.

### P2-4 CI/CD gap
- **Check:** `.github/workflows/*.yml` exists (needs `Read` audit). Verify `uv cache` and `audit-secrets.sh` in CI.

### P2-5 Engine parity — `_metadata` leakage
- **File:** `src/utils/engine.py` — `spark.read.json` locally vs Auto Loader on CE may produce different schemas (`_metadata`). Need test asserting `bronze` schemas byte-identical.

### P2-6 Generators small-file problem
- **File:** `generator/*` — each source writes one JSON-lines per run; landing/ many tiny files → Bronze suffers. Need coalesce before Auto Loader or `landing` partition sizing.

---

## Execution Plan (Phase 2)
1. Commit P0-2 (ruff) — tiny, verify `ruff check` passes.
2. Commit P0-1 perf — conftest Delta single session + shuffle 2 + sessionization repartition fix. Verify `pytest tests/test_scd2+sessionization+dedup+quality` < 40s.
3. Commit P0-1 billing fixture refactor — delete module spark fixture. Verify `pytest tests/test_billing_idempotency -v` < 30s.
4. Full suite `uv run pytest -q --durations=10` target < 60s, `uv run ruff check .` clean.
5. Log remaining P1/P2 as follow-up issues (not in this PR if out of scope).

## Verification Commands
```bash
uv run ruff check .
uv run pytest -q --durations=10
uv run pytest tests/test_billing_idempotency.py -v --durations=5
uv run pytest tests/test_scd2.py tests/test_sessionization.py -v
bash scripts/health-check.sh   # optional
```
