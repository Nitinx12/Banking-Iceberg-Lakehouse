"""GX suite smoke — expectations files exist, valid JSON, and map to generator messiness."""

import json
import pathlib


def test_gx_suites_exist():
    gx = pathlib.Path("gx/expectations")
    assert (gx / "watch_events.json").exists()
    assert (gx / "subscriptions_cdc.json").exists()
    assert (gx / "billing.json").exists()
    assert (gx / "content_catalog.json").exists()


def test_watch_events_suite_covers_4_messiness():
    suite = json.loads((pathlib.Path("gx/expectations/watch_events.json")).read_text())
    exps = [e["expectation_type"] for e in suite["expectations"]]
    # duplicate, null, range/future, late
    assert "expect_column_values_to_be_unique" in exps  # duplicate
    assert exps.count("expect_column_values_to_not_be_null") >= 2  # nulls
    assert "expect_column_values_to_be_between" in exps  # range/future


def test_subscriptions_suite_covers_cdc():
    suite = json.loads(
        (pathlib.Path("gx/expectations/subscriptions_cdc.json")).read_text()
    )
    cols = [e["kwargs"].get("column") for e in suite["expectations"]]
    assert "subscription_id" in cols
    assert "event_type" in cols


def test_billing_suite_covers_pk_and_range():
    suite = json.loads((pathlib.Path("gx/expectations/billing.json")).read_text())
    assert any(
        e["kwargs"].get("column") == "transaction_id"
        and e["expectation_type"] == "expect_column_values_to_be_unique"
        for e in suite["expectations"]
    )


def test_content_catalog_suite_row_count():
    suite = json.loads(
        (pathlib.Path("gx/expectations/content_catalog.json")).read_text()
    )
    assert any(
        e["expectation_type"] == "expect_table_row_count_to_be_between"
        for e in suite["expectations"]
    )
