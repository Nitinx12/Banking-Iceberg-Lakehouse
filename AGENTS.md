# StreamFlix Lakehouse — Agents

Project-scoped subagents for `streamflix-lakehouse`. Each agent is a Markdown file in `.claude/agents/` with YAML frontmatter (`name`, `description`, `tools`, `model`) + system prompt. Claude delegates automatically when your request matches `description`, or you can force it with `@agent-name`.

Location: `.claude/agents/` | Config: `.claude/settings.json` | Restart Claude Code after first adding `agents/` directory.

## Overview

| Agent | Tools | Model | Purpose |
|---|---|---|---|
| `pyspark-reviewer` | `Read`, `Grep`, `Glob`, `Bash` | `inherit` | Reviews Bronze/Silver/Gold notebooks and `src/` for idempotency, partitioning, schema evolution, and interview defensibility |
| `data-quality-auditor` | `Read`, `Grep`, `Glob` (read-only) | `sonnet` | Audits quality gate + quarantine table against the 4 messiness types the generator injects |
| `scd2-debugger` | `Read`, `Edit`, `Bash`, `Grep`, `Glob` | `inherit` | Diagnoses/fixes SCD2 merge bugs in `src/scd2.py` and re-proves idempotency |
| `interview-coach` | `Read`, `Grep`, `Glob` | `inherit` | Mock-interview rehearsal against README term-to-project mapping table |

> `pyspark-reviewer` and `data-quality-auditor` are intentionally read-only / near-read-only — a reviewer that silently fixes what it reviews defeats the point.

## Invocation

```text
@agent-name <task>
# examples
@pyspark-reviewer review notebooks/silver/02_clean_watch_events.py
@data-quality-auditor is the quality gate working?
@scd2-debugger tests/test_scd2.py is failing on duplicate history rows
@interview-coach quiz me on CDC
```

Or via slash command: `/interview-prep CDC` → delegates to `interview-coach`.

All agents run in their own context window; only the summary returns to the main conversation.

---

## `pyspark-reviewer`

**File:** `.claude/agents/pyspark-reviewer.md:1` | **Triggers:** after writing/modifying any `notebooks/bronze/*`, `notebooks/silver/*`, `notebooks/gold/*`, or `src/` module.

Reviews for *correctness* and *explainability* — can you defend the decision in an interview?

**Workflow:**
1. `git diff` to find changes (or review named files if no diff)
2. Infer medallion layer from path (`notebooks/bronze/`, `notebooks/silver/`, `notebooks/gold/`, `src/`)
3. Apply layer checklist below

**Bronze checklist** (`.claude/agents/pyspark-reviewer.md:15`):
- Auto Loader `cloudFiles` + `mergeSchema`, `trigger(availableNow=True)` (CE clusters auto-terminate ~1hr)
- Writes to `bronze` Hive DB (no Unity Catalog on CE), preserves raw fields

**Silver checklist** (`.claude/agents/pyspark-reviewer.md:21`):
- Dedupe on natural keys (`event_id` for `watch_events`)
- SCD2 `subscriptions` uses `MERGE`, idempotent (cross-check `src/scd2.py`)
- Via `src/quality_checks.py` → quarantine, not fail-batch; partitioned by date
- Handles late/duplicate/null/out-of-order events

**Gold checklist** (`.claude/agents/pyspark-reviewer.md:28`):
- Denormalized, answers business question (DAU/WAU, watch time by genre, churn, MRR)
- Broadcast joins for `content_catalog`, no row-level cleaning

**Cross-cutting** (`.claude/agents/pyspark-reviewer.md:33`): idempotency, partitioning/ZORDER alignment, DB routing (`bronze`/`silver`/`gold`), CE workaround documented.

**Output:** `Critical` / `Warnings` / `Suggestions` with offending code + concrete fix (`partitionBy(...)`).

## `data-quality-auditor`

**File:** `.claude/agents/data-quality-auditor.md:1` | **Triggers:** "is the quality gate working", quarantine verification, "how do you know your data is trustworthy?"

Read-only. Verifies `src/quality_checks.py` + `notebooks/silver/03_quality_gate.py` against generator-injected messiness (`generator/`):

- late-arriving events
- duplicate `event_id`s (`watch_events`)
- nulls in optional fields
- out-of-order timestamps

**Checks** (`.claude/agents/data-quality-auditor.md:18`):
1. Each messiness type has a catching check (explicitly call out missing)
2. Failures → quarantine table (with reason), not dropped / not fail-batch
3. Pass/fail counts logged (Definition of Done)
4. Freshness/SLA measured (e.g., "Gold refreshed within 2h of Bronze"), not just documented
5. `tests/test_quality_checks.py` exercises each check with deliberately bad record

**Output:** checklist `✅ / ⚠️ / ❌` per item with `file:line`, ending with single biggest gap (likely interview follow-up).

## `scd2-debugger`

**File:** `.claude/agents/scd2-debugger.md:1` | **Triggers:** SCD2 tests fail, history rows duplicated/missing `end_date`/wrong `is_current`, extending SCD2.

Fixes SCD2 for `subscriptions` in `src/scd2.py`. Source spec: CDC append-only with `event_type` (`insert`/`update`/`delete`), `subscription_id`, `user_id`, `plan_tier`, `status`, `change_timestamp`, `previous_plan_tier`.

**Workflow** (`.claude/agents/scd2-debugger.md:12`):
1. `uv run pytest tests/test_scd2.py -v` to reproduce
2. Read `src/scd2.py` `MERGE` logic
3. Diagnose: `is_current` (single current per key), `effective_date`/`end_date`, non-idempotent re-run, unhandled `delete`, unsorted `change_timestamp`
4. Minimal fix → re-run `uv run pytest tests/test_scd2.py -v` + **repeat-run idempotency test** (same batch twice → same row count, DoD requirement; add if missing)
5. Explain root cause for interview mapping table (Idempotency / SCD rows)

## `interview-coach`

**File:** `.claude/agents/interview-coach.md:1` | **Triggers:** "quiz me", "rehearse", "prep for interview", `/interview-prep [topic]`.

Friendly but rigorous mock interviewer on ~35 terms from README mapping table (data modeling, streaming, CDC, idempotency, partitioning, data quality, CI/CD, performance).

**Session** (`.claude/agents/interview-coach.md:10`):
1. Ask focus area (`data modeling` / `streaming` / `data quality` / `orchestration/CI` / `performance` / surprise) or take named term
2. One term at a time: explain concept **and** point to repo location — generic answer alone rejected
3. Verify with `Read`/`Grep`/`Glob` that file/pattern exists and matches
4. Verdict: strengths + what interviewer would push ("what with bigger budget?", "how prove idempotent?")
5. Conversational pace; follow-ups from README "Interview talking points", especially CE trade-offs and trustworthiness

---

## Related

- **Commands:** `.claude/commands/` — `/new-bronze-ingest`, `/new-silver-transform`, `/scd2-scaffold`, `/quality-check`, `/benchmark`, `/interview-prep` (`disable-model-invocation: true`)
- **Hooks:** `.claude/settings.json:2` — `PreToolUse` `protect-files.sh` (blocks `.env`/`uv.lock`/`landing/`/`raw/`), `PostToolUse` `format-python.sh` (`uv run ruff format/check --fix`), `SessionStart[compact]` reminder (`uv`, medallion DBs, `src/scd2.py`, `src/quality_checks.py`)
- **Docs to keep in sync:** `docs/lineage.md`, `docs/data_dictionary.md`, `docs/benchmarks.md`
- **Tooling:** `uv` only (not bare `pip`); `uv run pytest`, `uv run ruff` pre-approved in `settings.json:37`

## Customizing

- Add `docs/lineage.md` command if manual sync is frequent
- Tighten `protect-files.sh` for `dist/` / `.databricks/`
- Split `pyspark-reviewer` into `bronze/silver/gold-reviewer` if checklist grows — tradeoff: focus vs sync cost
