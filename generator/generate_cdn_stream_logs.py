"""Generate cdn_stream_logs — high-volume QoE telemetry with in-session gaps for sessionization.

Logs are grouped by session_id with playback positions advancing; occasional
>30-minute inactivity gaps force the Silver sessionizer to split sessions.
"""

import argparse
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(51)
random.seed(51)

CDN_EDGES = [
    "edge-us-east",
    "edge-us-west",
    "edge-eu-west",
    "edge-eu-central",
    "edge-ap-south",
]
DEVICE_TYPES = ["tv", "mobile", "web", "tablet", "console"]


def generate_cdn_stream_logs(
    n: int = 200_000,
    n_users: int = 5000,
    n_content: int = 1000,
    messiness: float = 0.02,
) -> list[dict]:
    """Build sessions of 5-30 logs each until ~n logs; inject QoE telemetry.

    Messiness: duplicate log_id (0.5%), null bitrate (2%), late-arriving
    timestamps shifted 7d earlier (3%), out-of-order shuffle (5%),
    future timestamps (0.1%).
    """
    user_ids = [f"user_{i:06d}" for i in range(n_users)]
    content_ids = [f"ct_{i:06d}" for i in range(n_content)]
    # 5-day window: matches the silver freshness gate (late_arriving_7d), so the
    # deliberate -7d "late" messiness is what trips the flag, not normal history
    base = datetime.now(tz=UTC) - timedelta(days=5)

    rows: list[dict] = []
    while len(rows) < n:
        session_id = f"sess_{uuid.uuid4().hex[:12]}"
        user_id = random.choice(user_ids)
        content_id = random.choice(content_ids)
        ts = base + timedelta(seconds=random.randint(0, 5 * 86400))
        position = 0.0
        n_logs = random.randint(5, 30)
        for i in range(n_logs):
            # gap: usually seconds-to-minutes; 5% of gaps exceed 30 min (session split)
            gap_s = (
                random.randint(30, 600)
                if random.random() > 0.05
                else random.randint(31 * 60, 4 * 3600)
            )
            ts = ts + timedelta(seconds=gap_s)
            position += gap_s * random.uniform(
                0.8, 1.1
            )  # playback roughly tracks wall clock
            row: dict = {
                "log_id": f"log_{uuid.uuid4().hex[:12]}",
                "session_id": session_id,
                "user_id": user_id,
                "content_id": content_id,
                "device_type": random.choice(DEVICE_TYPES),
                "event_timestamp": ts.isoformat(),
                "bitrate_kbps": random.choice(
                    [500, 1000, 2500, 5000, 8000, 12000, 15000]
                ),
                "rebuffer_ms": random.choice(
                    [0, 0, 0, 100, 250, 500, 1000, 2500, 5000]
                ),
                # startup latency only measured on the first log of a session
                "startup_ms": random.randint(300, 4000) if i == 0 else None,
                "cdn_edge": random.choice(CDN_EDGES),
                "playback_position_seconds": round(position, 1),
            }
            if random.random() < messiness:
                row["bitrate_kbps"] = None
            rows.append(row)

    rows = rows[:n]

    # duplicate log_id (0.5%)
    for r in random.sample(rows, int(len(rows) * 0.005)):
        rows.append(r.copy())

    # late-arriving: push 3% of timestamps 7 days earlier
    for r in random.sample(rows, int(len(rows) * 0.03)):
        ts = datetime.fromisoformat(r["event_timestamp"])
        r["event_timestamp"] = (ts - timedelta(days=7)).isoformat()

    # out-of-order: shuffle 5%
    for _ in range(int(len(rows) * 0.05)):
        i, j = random.sample(range(len(rows)), 2)
        rows[i], rows[j] = rows[j], rows[i]

    # future timestamps (anomaly) 0.1%
    for r in random.sample(rows, max(1, int(len(rows) * 0.001))):
        ts = datetime.fromisoformat(r["event_timestamp"])
        r["event_timestamp"] = (ts + timedelta(days=30)).isoformat()

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate cdn_stream_logs")
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--out", type=str, default="landing/cdn_stream_logs")
    args = parser.parse_args()

    rows = generate_cdn_stream_logs(n=args.rows)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"cdn_stream_logs_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
