<!-- Title MUST follow Conventional Commits — enforced by commitlint.yml
     and .githooks/commit-msg:
     feat(silver): ... | fix(bronze): ... | ci: ... | docs: ...
     area labels are auto-applied from changed paths (.github/labeler.yml) -->

## What & why

<!-- What changes, and the reason. Link the issue: Closes #123 -->

## Layer affected

- [ ] Bronze
- [ ] Silver
- [ ] Gold
- [ ] Not layer-specific (CI, tooling, docs)

## Checklist

- [ ] `uv run ruff check .` passes
- [ ] `uv run ruff format --check .` passes
- [ ] `uv run pytest` passes — tests added/updated for any behavior change
- [ ] `pre-commit run --all-files` passes
- [ ] No secrets, tokens, or `.env` content in the diff
- [ ] If schema/table changed: `docs/lineage.md` + `docs/data_dictionary.md` updated
