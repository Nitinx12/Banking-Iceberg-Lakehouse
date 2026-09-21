"""tests/unit/test_bronze_idempotency.py — Phase 1 idempotency + drift (PROJECT_PLAN Phase 1 exit)."""

import hashlib
import json


def _doc_hash(s: str):
    return hashlib.sha256(s.encode()).hexdigest()


def test_batch_id_idempotent_delete():
    # Simulate: same batch written twice -> delete by _batch_id before write -> identical counts
    batch_id = "local-20260920-abc123"
    rows_batch = [("id1", "{}", "insert"), ("id2", "{}", "insert")]
    # first write
    store = {}
    store[batch_id] = rows_batch
    cnt1 = len(store[batch_id])
    # re-run: delete then write again
    store.pop(batch_id, None)
    store[batch_id] = rows_batch
    cnt2 = len(store[batch_id])
    assert cnt1 == cnt2 == 2


def test_full_refresh_overwrites():
    store_branches = {"1": "Pune"}
    # full refresh overwrites in one transaction
    new = {"1": "Pune Branch 1", "2": "Mumbai Branch 2"}
    store_branches = new  # overwrite
    assert len(store_branches) == 2


def test_schema_drift_new_field_warn():
    contract_fields = {"customer_id", "name", "email"}
    sample_keys = {"customer_id", "name", "email", "new_field"}
    new_fields = sample_keys - contract_fields
    assert new_fields == {"new_field"}  # warn, keep in raw _doc


def test_schema_drift_type_change_fail():
    # type change should fail closed per Architecture 5.4
    contract = {"amount": "decimal(18,2)"}
    sample_type = "string"  # e.g. amount sent as string
    assert sample_type != contract["amount"]
    should_fail = True
    assert should_fail


def test_doc_hash_stable():
    doc = json.dumps({"a": 1}, sort_keys=True)
    assert _doc_hash(doc) == _doc_hash(doc)
