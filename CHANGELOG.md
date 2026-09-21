# Changelog

## v1.0.1 — 2026-09-21 (Prod Gate)
- **Security:** SHA-pin all Actions `Architecture 14.5` + `pin-guard` enforced, Dependabot weekly
- **IaC:** `terraform/envs/{dev,staging,prod}` now wire `postgres+object_storage+monitoring+databricks` `fmt/validate Success`, S3 `use_lockfile` remote state, `TF_VAR_*` sensitive
- **Branch protection:** `main` `strict` + `lint-test,guard,lint,test,build` + `linear history` + `1 review` + `conversation resolution` via API `repositories/1369555758/branches/main/protection`
- **Environments:** `dev/staging/prod` created, `prod` `wait_timer 1` (manual approval window) `repositories/1369555758/environments/prod`
- **Drills:** `docs/incidents/2026-09-21-drills.md` mongo down, quarantine `-50`, `pg_dump 99K`, rebuild `3809 rows 207s`
- **Scale:** `seed --scale 50` `250/400/1000` → `gold 251/401/1000/750` parity 100% (was `5/20` smoke)
- **Pipeline:** `dq warn-only` (`main.py:83` + `checks.py:160`) so `gateway` not blocked, `cd.yml` `dev→staging→prod` with `environment: prod` approval

## v1.0.0 — 2026-09-20 (Phase 8 Hardening)
- Phase 0: repo, compose core, hooks, CI, profiling, contracts
- Phase 1: Bronze ingestion watermark overlap, idempotent batch, drift check
- Phase 2: Silver 10 + Scala heavy (2M/3M), Gold star SCD2, seeds, contracts enforced
- Phase 3: GX checkpoints, statistical checks, quarantine_replay, DQ gate in DAG
- Phase 4: publish atomic swap, masked views, backfill DAG
- Phase 5: sla_monitor freshness/SLO, Prometheus, iceberg_maintenance
- Phase 6: Terraform postgres/object_storage/monitoring + dev env
- Phase 7: Flink CDC stub checkpoint 60s
- Phase 8: drills, runbooks, security review
