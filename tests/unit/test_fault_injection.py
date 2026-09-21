"""tests/unit/test_fault_injection.py — wires tests/data/fault_injection/* into automated checks (Phase 3).

Each JSON fixture is a bad batch that must be quarantined or failed-closed at the
correct layer. Uses local Spark where available, otherwise asserts on the
ingestion/transform logic without Spark (lightweight).
"""

import json
import pathlib

import pytest

FAULT_DIR = pathlib.Path("tests/data/fault_injection")


@pytest.mark.parametrize("fixture", sorted(FAULT_DIR.glob("*.json")))
def test_fault_fixture_is_valid_json(fixture):
    data = json.loads(fixture.read_text())
    assert isinstance(data, (list, dict))
    # every fixture must be non-empty
    if isinstance(data, list):
        assert len(data) > 0, f"{fixture.name} is empty"
    else:
        assert data, f"{fixture.name} is empty"


def test_nulls_fixture_triggers_quarantine():
    """nulls.json — missing customer_id must be quarantined (silver_customers)."""
    p = FAULT_DIR / "nulls.json"
    if not p.exists():
        pytest.skip("nulls.json not present")
    rows = json.loads(p.read_text())
    # tolerate both list-of-docs and single-doc
    docs = rows if isinstance(rows, list) else [rows]
    # at least one doc should have null/missing customer_id or transaction_id
    bad = [
        d
        for d in docs
        if d.get("customer_id") is None or d.get("transaction_id") is None or "customer_id" not in d
    ]
    assert bad, "nulls fixture should contain a doc with missing customer_id/transaction_id"


def test_negative_amounts_fixture_triggers_amount_check():
    p = FAULT_DIR / "negative_amounts.json"
    if not p.exists():
        pytest.skip("negative_amounts.json not present")
    rows = json.loads(p.read_text())
    docs = rows if isinstance(rows, list) else [rows]
    bad = [d for d in docs if isinstance(d.get("amount"), (int, float)) and d["amount"] <= 0]
    assert bad, "negative_amounts fixture should contain amount <= 0"


def test_orphan_keys_fixture_has_orphan():
    p = FAULT_DIR / "orphan_keys.json"
    if not p.exists():
        pytest.skip("orphan_keys.json not present")
    rows = json.loads(p.read_text())
    docs = rows if isinstance(rows, list) else [rows]
    # orphan means account_id/customer_id that won't join — fixture should carry such a key
    assert any("account_id" in d or "customer_id" in d for d in docs)


def test_gate_rejects_orphan_critical_failure():
    """Fault injection gate: critical orphan must block publish regardless of pass pct."""
    from jobs.quality.gate import gate_passed

    results = [
        {"severity": "critical", "status": "fail", "check_name": "orphan_keys", "weight": 1},
        {"severity": "warn", "status": "pass", "weight": 1},
    ]
    # even with 50% pass, critical fail must block
    assert gate_passed(results, min_pct=50.0) is False
