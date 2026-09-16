"""Sessionization tests — gap boundaries, single events, out-of-order input, rollups."""
import pytest

from src.core.sessionization import rollup_sessions, sessionize

pytestmark = pytest.mark.slow

LOG_SCHEMA = [
    "log_id",
    "session_id",
    "user_id",
    "content_id",
    "device_type",
    "event_timestamp",
    "bitrate_kbps",
    "rebuffer_ms",
    "startup_ms",
]


def test_gap_exactly_30min_same_session(spark):
    df = spark.createDataFrame(
        [
            ("l1", "s1", "u1", "ct_1", "tv", "2024-01-01T10:00:00+00:00", 5000, 0, 800),
            ("l2", "s1", "u1", "ct_1", "tv", "2024-01-01T10:30:00+00:00", 5000, 100, None),
        ],
        schema=LOG_SCHEMA,
    )
    result = sessionize(df).orderBy("event_timestamp").collect()
    assert [r["session_number"] for r in result] == [1, 1]


def test_gap_over_30min_starts_new_session(spark):
    df = spark.createDataFrame(
        [
            ("l1", "s1", "u1", "ct_1", "tv", "2024-01-01T10:00:00+00:00", 5000, 0, 800),
            ("l2", "s1", "u1", "ct_1", "tv", "2024-01-01T10:30:01+00:00", 5000, 100, None),
        ],
        schema=LOG_SCHEMA,
    )
    result = sessionize(df).orderBy("event_timestamp").collect()
    assert [r["session_number"] for r in result] == [1, 2]


def test_single_event_is_session_one(spark):
    df = spark.createDataFrame(
        [("l1", "s1", "u1", "ct_1", "tv", "2024-01-01T10:00:00+00:00", 5000, 0, 800)],
        schema=LOG_SCHEMA,
    )
    result = sessionize(df).collect()
    assert result[0]["session_number"] == 1


def test_sessions_independent_per_key(spark):
    df = spark.createDataFrame(
        [
            ("l1", "s1", "u1", "ct_1", "tv", "2024-01-01T10:00:00+00:00", 5000, 0, 800),
            ("l2", "s2", "u2", "ct_1", "tv", "2024-01-01T10:00:00+00:00", 5000, 0, 800),
        ],
        schema=LOG_SCHEMA,
    )
    result = sessionize(df).collect()
    assert all(r["session_number"] == 1 for r in result)


def test_out_of_order_input_same_assignment(spark):
    # same events as test_gap_over_30min... but delivered out of order
    df = spark.createDataFrame(
        [
            ("l2", "s1", "u1", "ct_1", "tv", "2024-01-01T10:30:01+00:00", 5000, 100, None),
            ("l1", "s1", "u1", "ct_1", "tv", "2024-01-01T10:00:00+00:00", 5000, 0, 800),
        ],
        schema=LOG_SCHEMA,
    )
    result = sessionize(df).orderBy("log_id").collect()
    assert {r["log_id"]: r["session_number"] for r in result} == {"l1": 1, "l2": 2}


def test_rollup_sessions_metrics(spark):
    df = spark.createDataFrame(
        [
            ("l1", "s1", "u1", "ct_1", "tv", "2024-01-01T10:00:00+00:00", 5000, 0, 800),
            ("l2", "s1", "u1", "ct_1", "tv", "2024-01-01T10:10:00+00:00", 8000, 250, None),
            ("l3", "s1", "u1", "ct_1", "tv", "2024-01-01T11:00:00+00:00", 5000, 100, None),  # new session
        ],
        schema=LOG_SCHEMA,
    )
    sessions = rollup_sessions(sessionize(df)).orderBy("session_number").collect()
    assert len(sessions) == 2

    first = sessions[0]
    assert first["n_logs"] == 2
    assert first["total_rebuffer_ms"] == 250
    assert first["startup_ms"] == 800  # startup = first log of the session
    assert first["avg_bitrate_kbps"] == 6500.0
    # compare as durations — the Spark session may render timestamps in local tz
    assert (first["session_end"] - first["session_start"]).total_seconds() == 600

    second = sessions[1]
    assert second["n_logs"] == 1
    assert second["session_date"].isoformat() == "2024-01-01"