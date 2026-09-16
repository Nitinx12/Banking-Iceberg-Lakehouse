"""Run GX validations locally on Spark — mirrors src/core/quality_checks.py quarantine path."""

from __future__ import annotations

import json
import pathlib


def load_suite(name: str) -> dict:
    p = pathlib.Path(__file__).parents[1] / "expectations" / f"{name}.json"
    return json.loads(p.read_text())


def validate_watch_events(df):
    """Example Spark validation using suite — delegates to quality_checks for quarantine."""
    from src.core.quality_checks import check_watch_events

    res = check_watch_events(df)
    suite = load_suite("watch_events")
    return {
        "suite": suite["expectation_suite_name"],
        "pass": res.pass_count,
        "fail": res.fail_count,
        "reasons": res.reasons,
    }


if __name__ == "__main__":
    print(
        "GX suites:",
        [
            p.stem
            for p in (pathlib.Path(__file__).parents[1] / "expectations").glob("*.json")
        ],
    )
