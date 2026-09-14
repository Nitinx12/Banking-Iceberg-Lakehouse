"""Generate watch_events — high-volume clickstream with intentional messiness."""

import argparse
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(44)
random.seed(44)

EVENT_TYPES = ["play", "pause", "seek", "stop"]
DEVICE_TYPES = ["tv", "mobile", "web", "tablet"]


def generate_watch_events(
    n: int = 100_000,
    n_users: int = 5000,
    n_content: int = 1000,
    messiness: float = 0.02,
) -> list[dict]:
    """Messiness injected: duplicates, late arrivals, nulls, out-of-order timestamps."""
    user_ids = [f"user_{i:06d}" for i in range(n_users)]
    content_ids = [f"ct_{i:06d}" for i in range(n_content)]
    # 5-day window: matches the silver freshness gate (late_arriving_7d), so the
    # deliberate -7d "late" messiness is what trips the flag, not normal history
    base = datetime.now(tz=UTC) - timedelta(days=5)

    rows: list[dict] = []
    for _ in range(n):
        ts = base + timedelta(seconds=random.randint(0, 5 * 86400))
        row: dict = {
            "event_id": f"evt_{uuid.uuid4().hex}",
            "user_id": random.choice(user_ids),
            "content_id": random.choice(content_ids),
            "event_type": random.choice(EVENT_TYPES),
            "event_timestamp": ts.isoformat(),
            "watch_duration_seconds": random.randint(0, 7200),
            "device_type": random.choice(DEVICE_TYPES),
            "session_id": f"sess_{uuid.uuid4().hex[:12]}",
        }
        # nulls in optional fields
        if random.random() < messiness:
            row["device_type"] = None
        if random.random() < messiness / 2:
            row["watch_duration_seconds"] = None
        rows.append(row)

    # duplicate event_ids (0.5%)
    n_dupes = int(n * 0.005)
    dupes = random.sample(rows, n_dupes)
    for d in dupes:
        rows.append(d.copy())

    # late-arriving events: push 1% of timestamps 7 days earlier
    for r in random.sample(rows, int(len(rows) * 0.01)):
        ts = datetime.fromisoformat(r["event_timestamp"])
        r["event_timestamp"] = (ts - timedelta(days=7)).isoformat()

    # out-of-order: shuffle 2%
    for _ in range(int(len(rows) * 0.02)):
        i, j = random.sample(range(len(rows)), 2)
        rows[i], rows[j] = rows[j], rows[i]

    # future timestamps (anomaly) 0.1%
    for r in random.sample(rows, max(1, int(len(rows) * 0.001))):
        ts = datetime.fromisoformat(r["event_timestamp"])
        r["event_timestamp"] = (ts + timedelta(days=30)).isoformat()

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate watch_events")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--out", type=str, default="landing/watch_events")
    args = parser.parse_args()

    rows = generate_watch_events(n=args.rows)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"watch_events_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
