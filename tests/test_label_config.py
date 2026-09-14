"""CI/labeling config sanity — fail fast when workflow or label config drifts.

Guards:
  1. Every .github/workflows/*.yml parses and follows repo standards
     (explicit top-level permissions, timeout-minutes on every job).
  2. .github/labeler.yml globs are anchored to real repo paths.
  3. Every label referenced in labeler.yml exists in labels.yml —
     actions/labeler does not auto-create labels, so a typo silently
     disables that rule. (label-sync.yml provisions labels from labels.yml.)
"""

from pathlib import Path

from ruamel.yaml import YAML

REPO_ROOT = Path(__file__).resolve().parents[1]
yaml = YAML(typ="safe")


def _load(path: Path):
    return yaml.load(path.read_text(encoding="utf-8"))


def _workflow_files():
    return sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml"))


def test_workflows_parse_and_follow_standards():
    files = _workflow_files()
    assert files, "no workflow files found"
    for wf in files:
        data = _load(wf)
        assert "jobs" in data, f"{wf.name}: missing jobs"
        assert "permissions" in data, f"{wf.name}: missing top-level permissions"
        for job_name, job in data["jobs"].items():
            assert job.get("timeout-minutes"), (
                f"{wf.name}: job '{job_name}' missing timeout-minutes"
            )


def _base_path(glob: str) -> str:
    """Truncate a glob at its first wildcard to get the anchored prefix."""
    for i, ch in enumerate(glob):
        if ch in "*?[":
            return glob[:i].rstrip("/")
    return glob


def test_labeler_globs_anchored_to_real_paths():
    labeler = _load(REPO_ROOT / ".github" / "labeler.yml")
    for label, rules in labeler.items():
        for rule in rules:
            for pattern in rule["changed-files"][0]["any-glob-to-any-file"]:
                base = _base_path(pattern)
                if not base:  # unanchored glob like "*.md" — nothing to check
                    continue
                assert (REPO_ROOT / base).exists(), (
                    f"label '{label}' glob '{pattern}' anchored to missing path '{base}'"
                )


def test_labeler_labels_exist_in_taxonomy():
    labeler = _load(REPO_ROOT / ".github" / "labeler.yml")
    labels = _load(REPO_ROOT / ".github" / "labels.yml")
    defined = {entry["name"] for entry in labels}
    for label, rules in labeler.items():
        assert label in defined, f"labeler.yml references undefined label: {label!r}"
        assert rules, f"label {label!r} has no matching rules"


def _defined_labels():
    labels = _load(REPO_ROOT / ".github" / "labels.yml")
    return {entry["name"] for entry in labels}


def test_issue_template_labels_exist_in_taxonomy():
    templates = list((REPO_ROOT / ".github" / "ISSUE_TEMPLATE").glob("*.md"))
    assert templates, "no issue templates found"
    for tmpl in templates:
        text = tmpl.read_text(encoding="utf-8")
        if not text.startswith("---"):
            continue
        frontmatter = text.split("---", 2)[1]
        meta = yaml.load(frontmatter)
        for label in meta.get("labels", []):
            assert label in _defined_labels(), (
                f"{tmpl.name} references undefined label: {label!r}"
            )


def test_issue_template_config_parses():
    config = _load(REPO_ROOT / ".github" / "ISSUE_TEMPLATE" / "config.yml")
    assert "blank_issues_enabled" in config


def test_dependabot_labels_exist_in_taxonomy():
    dependabot = _load(REPO_ROOT / ".github" / "dependabot.yml")
    assert dependabot["updates"], "dependabot.yml has no update configs"
    for update in dependabot["updates"]:
        for label in update.get("labels", []):
            assert label in _defined_labels(), (
                f"dependabot references undefined label: {label!r} "
                "(run label-sync.yml to provision it)"
            )


def test_codeowners_paths_exist():
    owners = REPO_ROOT / ".github" / "CODEOWNERS"
    assert owners.exists()
    for line in owners.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        rule = line.split()[0]
        if rule == "*":
            continue
        path = rule.lstrip("/").rstrip("/")
        assert (REPO_ROOT / path).exists(), (
            f"CODEOWNERS rule targets missing path: {rule}"
        )
