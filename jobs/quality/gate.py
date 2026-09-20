"""jobs/quality/gate.py — DQ gate (Architecture 11.3).

dq_score = weighted passed / weighted total per layer. Critical checks must be 100% regardless of score.
Gate threshold via DQ_GATE_MIN_PASS_PCT.
"""

import os

from jobs.quality.checks import dq_score


def gate_passed(results: list[dict], min_pct: float = None) -> bool:
    min_pct = min_pct if min_pct is not None else float(os.getenv("DQ_GATE_MIN_PASS_PCT", "98.0"))
    critical_failed = any(r["severity"] == "critical" and r["status"] == "fail" for r in results)
    if critical_failed:
        return False
    total = sum(r.get("weight", 1) for r in results)
    passed = sum(r.get("weight", 1) for r in results if r["status"] == "pass")
    return dq_score(passed, total) >= min_pct
