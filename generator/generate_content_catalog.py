"""Generate content_catalog — slow-changing dimension (~2-5k rows)."""

import argparse
import json
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

GENRES = ["Drama", "Comedy", "Action", "Sci-Fi", "Documentary", "Horror", "Romance", "Thriller"]
CONTENT_TYPES = ["movie", "series"]
RATINGS = ["G", "PG", "PG-13", "R", "NC-17"]


def generate_content_catalog(n: int = 3000, messiness: float = 0.02) -> list[dict]:
    rows = []
    for _ in range(n):
        release = fake.date_between(start_date="-10y", end_date="-1d")
        row: dict = {
            "content_id": f"ct_{uuid.uuid4().hex[:8]}",
            "title": fake.catch_phrase(),
            "genre": random.choice(GENRES),
            "release_date": release.isoformat(),
            "content_type": random.choice(CONTENT_TYPES),
            "runtime_minutes": random.randint(22, 210),
            "rating": random.choice(RATINGS),
            "added_at": (release + timedelta(days=random.randint(1, 30))).isoformat(),
        }
        # intentional messiness: nulls in optional fields
        if random.random() < messiness:
            row["rating"] = None
        if random.random() < messiness / 2:
            row["genre"] = None
        rows.append(row)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate content_catalog")
    parser.add_argument("--rows", type=int, default=3000)
    parser.add_argument("--out", type=str, default="landing/content_catalog")
    args = parser.parse_args()

    rows = generate_content_catalog(n=args.rows)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"content_catalog_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
