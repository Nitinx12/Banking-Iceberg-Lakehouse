# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| `main`  | ✅ (pre-1.0 portfolio project — fixes land on `main` only) |

## Reporting a vulnerability

**Do not open a public issue or PR for anything security-related.**

Instead, either:

1. **GitHub private vulnerability reporting** (preferred): this repo's
   **Security → Report a vulnerability** button, or
2. Email the maintainer: **nitin321x@gmail.com** (address is also in
   `pyproject.toml`), with details and reproduction steps.

Please redact any credentials/tokens from anything you share. Expect an
acknowledgement within **72 hours**; resolution timelines depend on
severity. Coordinated disclosure is fine — just ask for a CVE timeline
first.

## Scope notes for this repo

- `DATABRICKS_HOST` / `DATABRICKS_TOKEN` live in `.env` (gitignored,
  blocked by `.githooks/pre-commit` and `detect-private-key`). If you
  find a way any secret can leak into CI logs or artifacts, report it
  under this policy.
- Third-party surface: GitHub Actions (see `.github/dependabot.yml` for
  update policy) — a compromised action update is in scope.

## Current security watch items

Tracked with the rest of the open issues in
[docs/STATUS.md](docs/STATUS.md); security-relevant highlights:

- CE personal access tokens are stored locally in `.env` — a stand-in for
  Databricks Secrets + KMS. Rotate the token if it was ever shared or
  pasted anywhere.
- GitHub Actions use least-privilege top-level permissions (`contents:
  read`); the only `pull_request_target` workflows (`label.yml`,
  `greetings.yml`) never check out PR code — this rule is enforced by
  `tests/test_workflows.py`, and adding a checkout there will fail CI.
- `scripts/monitor-ci.sh` needs an authenticated `gh` CLI — its reports
  (written under `.reports/`, gitignored) contain run metadata, not
  secrets.
