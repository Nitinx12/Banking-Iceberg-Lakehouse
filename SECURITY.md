# Security Policy

## Scope

This is a portfolio platform built on synthetic data. The controls are designed as if the data were real (see `Architecture.md` §14) — treat any leak-class bug in them as reportable.

**Not in scope:** the bundled sample datasets, anything under `tests/data/`, or the demo dashboard content — none of it is real customer data.

## Reporting a vulnerability

Use GitHub's **private security advisory** flow: *Security → Advisories → New draft security advisory* on this repository.

Please do not open a public issue for anything that looks exploitable.

Include: affected component (ingestion / transform / quality / serving / orchestration / IaC), reproduction steps, and impact. You'll get an acknowledgement within 3 business days.

## What we never want in the repo

- `.env` contents, database passwords, Mongo credentials, MinIO keys
- Databricks tokens, cloud keys, Slack webhooks, HMAC salts
- Anything from a real customer dataset

`gitleaks` runs on every commit (hook + CI) and blocks known patterns, but new patterns slip past scanners — review your own diff before pushing.

## Secrets handling quick reference

| Where | How |
|---|---|
| Local dev | `.env` (gitignored), generated via `make env` |
| CI | GitHub Environment secrets + OIDC federation, no long-lived cloud keys (Architecture 14.1) |
| Databricks | Secret scopes managed by Terraform |
| Rotation | DB passwords every 90 days, tokens every 30 days |
