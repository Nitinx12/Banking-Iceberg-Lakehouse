"""content_ratings upsert semantics — latest-wins on (user_id, content_id).

Re-ratings carry a NEW rating_id, so PK dedupe would keep both; the natural
key of this table is (user_id, content_id) with latest updated_at winning.
"""

from src.core.transformations import latest_rating_per_user_content

RATING_SCHEMA = ["rating_id", "user_id", "content_id", "rating", "review_text", "rated_at", "updated_at"]


def test_latest_rating_wins(spark):
    df = spark.createDataFrame(
        [
            ("rt_old", "user_1", "ct_1", 2, "meh", "2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"),
            ("rt_new", "user_1", "ct_1", 5, "actually great", "2024-01-01T00:00:00+00:00", "2024-02-01T00:00:00+00:00"),
        ],
        schema=RATING_SCHEMA,
    )
    result = latest_rating_per_user_content(df).collect()
    assert len(result) == 1
    assert result[0]["rating_id"] == "rt_new"
    assert result[0]["rating"] == 5


def test_distinct_user_content_pairs_kept(spark):
    df = spark.createDataFrame(
        [
            ("rt_1", "user_1", "ct_1", 4, "good", "2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"),
            ("rt_2", "user_1", "ct_2", 3, "fine", "2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"),
            ("rt_3", "user_2", "ct_1", 5, "great", "2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"),
        ],
        schema=RATING_SCHEMA,
    )
    result = latest_rating_per_user_content(df)
    assert result.count() == 3


def test_latest_wins_idempotent_rerun(spark):
    df = spark.createDataFrame(
        [
            ("rt_old", "user_1", "ct_1", 2, "meh", "2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"),
            ("rt_new", "user_1", "ct_1", 5, "great", "2024-01-01T00:00:00+00:00", "2024-02-01T00:00:00+00:00"),
        ],
        schema=RATING_SCHEMA,
    )
    once = latest_rating_per_user_content(df)
    twice = latest_rating_per_user_content(once)
    assert once.count() == twice.count() == 1
