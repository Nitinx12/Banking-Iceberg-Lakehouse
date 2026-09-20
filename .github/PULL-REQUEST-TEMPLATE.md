<!-- Definition of done (AGENTS.md §15): code + tests + docs in the same PR,
     lint passes, CI green, no secrets, new stages emit ops.pipeline_runs rows,
     new alerts ship with a runbook. -->

## What & why

<!-- One or two sentences: what changed and the motivation. Reference the
     PROJECT_PLAN phase / ADR when relevant. -->

## How tested

- [ ] Unit tests added/updated (`make test_fast`)
- [ ] Integration impact checked (`pytest -m integration` needs compose core up)
- [ ] `make lint` passes locally (hooks run it anyway)

## Checklist

- [ ] Idempotent: re-running the change for the same window is safe
- [ ] No secrets or environment-specific values committed (gitleaks gates this)
- [ ] Docs/ADRs updated if a decision changed
- [ ] Any new alert has a runbook in `docs/runbooks/`
