"""Generate content_ratings — user reviews with re-ratings (upsert semantics, latest wins)."""

import argparse
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(52)
random.seed(52)


def generate_content_ratings(
    n: int = 20_000,
    n_users: int = 5000,
    n_content: int = 1000,
    rerate_pct: float = 0.05,
    messiness: float = 0.005,
) -> list[dict]:
    """Ratings per (user, content). 5% get a later re-rating with a new rating_id —
    Silver must dedupe on (user_id, content_id) latest-wins, NOT on rating_id PK.

    Messiness: rating out of range (0.5%), null user_id (0.5%).
    """
    user_ids = [f"user_{i:06d}" for i in range(n_users)]
    content_ids = [f"ct_{i:06d}" for i in range(n_content)]
    base = datetime.now(tz=UTC) - timedelta(days=365)

    rows: list[dict] = []
    for _ in range(n):
        rated_at = base + timedelta(seconds=random.randint(0, 365 * 86400))
        row: dict = {
            "rating_id": f"rt_{uuid.uuid4().hex[:12]}",
            "user_id": random.choice(user_ids),
            "content_id": random.choice(content_ids),
            "rating": random.choices(
                [1, 2, 3, 4, 5], weights=[0.05, 0.10, 0.25, 0.35, 0.25]
            )[0],
            "review_text": fake.sentence(nb_words=10)
            if random.random() < 0.4
            else None,
            "rated_at": rated_at.isoformat(),
            "updated_at": rated_at.isoformat(),
        }
        if random.random() < messiness:
            row["rating"] = random.choice([0, 6])  # out of range
        if random.random() < messiness:
            row["user_id"] = None  # null FK messiness
        rows.append(row)

    # re-ratings: same user+content, new rating_id, later updated_at (latest wins)
    for r in random.sample(rows, int(len(rows) * rerate_pct)):
        rerated_at = datetime.fromisoformat(r["updated_at"]) + timedelta(
            days=random.randint(1, 60)
        )
        rows.append(
            {
                "rating_id": f"rt_{uuid.uuid4().hex[:12]}",
                "user_id": r["user_id"],
                "content_id": r["content_id"],
                "rating": random.randint(1, 5),
                "review_text": fake.sentence(nb_words=10)
                if random.random() < 0.4
                else None,
                "rated_at": r["rated_at"],
                "updated_at": rerated_at.isoformat(),
            }
        )

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate content_ratings")
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--out", type=str, default="landing/content_ratings")
    args = parser.parse_args()

    rows = generate_content_ratings(n=args.rows)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"content_ratings_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
