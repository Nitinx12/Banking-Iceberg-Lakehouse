"""Unit tests for .github/workflows — the contracts every push depends on.

test_label_config.py covers parse + repo standards (top-level permissions,
timeout-minutes). These tests cover behaviour instead: triggers, the steps
CI actually runs, action pinning, and the pull_request_target safety rule.
They run as part of `pytest` in ci.yml on every push, so a broken workflow
file fails the build before GitHub ever schedules it.
"""

import re
from pathlib import Path

from ruamel.yaml import YAML

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
yaml = YAML(typ="safe")

REQUIRED_WORKFLOWS = {
    "ci.yml",
    "nightly-e2e.yml",
    "commitlint.yml",
    "codacy.yml",
    "label.yml",
    "label-sync.yml",
    "greetings.yml",
    "summary.yml",
}

# pull_request_target grants a write token against the BASE branch — the risk
# is checking out / executing untrusted PR code. labeler and first-interaction
# only read the PR via API, so they're allowed; any checkout there is a bug.
PR_TARGET_NO_CHECKOUT_ALLOWLIST = {"label.yml", "greetings.yml"}


def _load_all() -> dict[str, dict]:
    files = sorted(WORKFLOWS.glob("*.yml"))
    assert files, "no workflow files found"
    return {p.name: yaml.load(p.read_text(encoding="utf-8")) for p in files}


def _triggers(data: dict) -> set[str]:
    """Normalize the `on:` block — YAML parses the bare key as boolean True."""
    on = data.get("on", data.get(True))
    if on is None:
        return set()
    if isinstance(on, str):
        return {on}
    if isinstance(on, list):
        return set(on)
    return set(on.keys())


def _steps(data: dict):
    for job in data["jobs"].values():
        yield from job.get("steps", [])


def _step_runs(data: dict) -> list[str]:
    return [s.get("run", "") for s in _steps(data) if "run" in s]


def test_required_workflows_exist():
    present = {p.name for p in WORKFLOWS.glob("*.yml")}
    missing = REQUIRED_WORKFLOWS - present
    assert not missing, f"missing required workflows: {sorted(missing)}"


def test_ci_runs_on_every_push_and_pr():
    data = _load_all()["ci.yml"]
    triggers = _triggers(data)
    assert "push" in triggers, "ci.yml must run on every push"
    assert "pull_request" in triggers, "ci.yml must run on PRs"


def test_ci_lints_and_tests():
    runs = _step_runs(_load_all()["ci.yml"])
    assert any("ruff check" in r for r in runs), "ci.yml must lint (ruff check)"
    assert any("pytest" in r for r in runs), "ci.yml must run the test suite"


def test_ci_uses_frozen_lockfile():
    runs = _step_runs(_load_all()["ci.yml"])
    assert any("uv sync --frozen" in r for r in runs), (
        "ci.yml must sync from the frozen lockfile, not resolve live"
    )


def test_nightly_is_cold_and_retriggerable():
    data = _load_all()["nightly-e2e.yml"]
    triggers = _triggers(data)
    assert "schedule" in triggers, "nightly E2E must run on a schedule"
    assert "workflow_dispatch" in triggers, "nightly E2E must be manually runnable"

    # cold runner by design — a warm cache defeats the point of the nightly
    steps = list(_steps(data))
    assert not any(s.get("with", {}).get("enable-cache") is True for s in steps), (
        "nightly E2E must not use a warm uv cache"
    )

    # generate must precede the suite: the suite consumes generated data
    run_idx = {i: s.get("run", "") for i, s in enumerate(steps) if "run" in s}
    gen = [i for i, r in run_idx.items() if "generate" in r]
    tst = [i for i, r in run_idx.items() if "pytest" in r]
    assert gen and tst, "nightly E2E must generate data and run pytest"
    assert min(gen) < min(tst), "nightly E2E must generate before testing"


def test_commitlint_gates_prs():
    data = _load_all()["commitlint.yml"]
    assert "pull_request" in _triggers(data), "commitlint must run on PRs"
    uses = [s.get("uses", "") for s in _steps(data)]
    assert any("commitlint" in u for u in uses), (
        "commitlint must use a commitlint action"
    )


def test_actions_are_pinned():
    """Every `uses:` must be owner/repo@tag or owner/repo@full-sha — never a branch."""
    # allows first-party subaction paths, e.g. github/codeql-action/upload-sarif@v3
    pattern = re.compile(r"^[\w.-]+/[\w.-]+(?:/[\w.-]+)?@(.+)$")
    for name, data in _load_all().items():
        for step in _steps(data):
            ref = step.get("uses")
            if not ref or ref.startswith("docker://"):
                continue
            match = pattern.match(ref)
            assert match, f"{name}: unpinned action reference {ref!r}"
            version = match.group(1)
            is_tag = re.fullmatch(r"v\d+(\.\d+)*", version)
            is_sha = re.fullmatch(r"[0-9a-f]{40}", version)
            assert is_tag or is_sha, (
                f"{name}: action {ref!r} pinned to a moving ref "
                f"(branch or short sha) — use a version tag or full commit sha"
            )


def test_pull_request_target_workflows_never_check_out():
    for name, data in _load_all().items():
        if "pull_request_target" not in _triggers(data):
            continue
        uses = [s.get("uses", "") for s in _steps(data)]
        assert not any("checkout" in u for u in uses), (
            f"{name}: pull_request_target workflow must not check out PR code "
            f"(write token + untrusted code = classic takeover)"
        )


def test_push_workflows_are_read_only():
    """Top-level permissions of push-triggered workflows must be read-only."""
    for name, data in _load_all().items():
        if "push" not in _triggers(data):
            continue
        perms = data.get("permissions", {})
        if isinstance(perms, str):
            perms = {"contents": perms}
        escalated = {scope: val for scope, val in perms.items() if val == "write"}
        assert not escalated, (
            f"{name}: push-triggered workflow grants top-level write: {escalated} "
            f"(escalate per-job instead)"
        )
