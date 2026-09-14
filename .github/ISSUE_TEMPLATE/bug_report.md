---
name: Bug report
about: Something isn't working — pipeline job, CI, or tooling
labels: ["bug"]
title: ""
---

## What broke?

<!-- Clear description of the bug. -->

## Which layer?

- [ ] Bronze (ingest)
- [ ] Silver (dedupe / SCD2 / quality gate)
- [ ] Gold (aggregates)
- [ ] Not layer-specific (CI, tooling, data generator)

## Environment

<!-- Local (uv, Spark version) or Databricks (DBR version, cluster type). -->

## Steps to reproduce

```bash
# commands, notebook, or job run
```

## Expected vs actual

| | |
|---|---|
| Expected | |
| Actual | |

## Logs / error

```
paste the relevant traceback or log lines
```
