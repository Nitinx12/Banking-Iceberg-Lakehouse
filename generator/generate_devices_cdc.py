"""Generate devices CDC change events (Debezium-style) — second SCD2 dimension.

Tracks user device fleet changes: OS/app version bumps, primary-device flips,
device removals. Exercises the generalized SCD2 logic in src/core/scd2.py.
"""

import argparse
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(46)
random.seed(46)

DEVICE_TYPES = ["tv", "mobile", "web", "tablet", "console"]
OS_FAMILIES = ["android", "ios", "tvos", "windows", "webos", "tizen"]
EVENT_TYPES = ["insert", "update", "delete"]


def generate_devices_cdc(
    n_users: int = 5000,
    updates_per_device: int = 3,
    messiness: float = 0.02,
) -> list[dict]:
    """Emit CDC events per (user, device) sorted randomly to simulate out-of-order delivery.

    Messiness: null os_version, out-of-order change_timestamp.
    """
    rows: list[dict] = []
    base = datetime.now(tz=UTC) - timedelta(days=365)

    for i in range(n_users):
        user_id = f"user_{i:06d}"
        for _ in range(random.randint(1, 3)):
            device_id = f"dev_{uuid.uuid4().hex[:10]}"
            device_type = random.choice(DEVICE_TYPES)
            os_family = random.choice(OS_FAMILIES)
            os_version = f"{random.randint(10, 17)}.{random.randint(0, 5)}"
            app_version = f"3.{random.randint(0, 9)}.{random.randint(0, 20)}"
            ts = base + timedelta(days=random.randint(0, 300))

            rows.append(
                {
                    "event_type": "insert",
                    "device_id": device_id,
                    "user_id": user_id,
                    "device_type": device_type,
                    "os_family": os_family,
                    "os_version": os_version,
                    "app_version": app_version,
                    "is_primary": random.random() < 0.3,
                    "change_timestamp": ts.isoformat(),
                }
            )

            for _ in range(random.randint(0, updates_per_device)):
                ts = ts + timedelta(days=random.randint(5, 90))
                event_type = random.choices(EVENT_TYPES, weights=[0.05, 0.85, 0.1])[0]
                if event_type == "update":
                    # bump versions or flip primary device
                    if random.random() < 0.7:
                        os_version = f"{random.randint(10, 18)}.{random.randint(0, 6)}"
                    if random.random() < 0.5:
                        app_version = (
                            f"3.{random.randint(0, 10)}.{random.randint(0, 25)}"
                        )
                    is_primary = random.random() < 0.2
                else:
                    is_primary = False
                row: dict = {
                    "event_type": event_type,
                    "device_id": device_id,
                    "user_id": user_id,
                    "device_type": device_type,
                    "os_family": os_family,
                    "os_version": os_version,
                    "app_version": app_version,
                    "is_primary": is_primary,
                    "change_timestamp": ts.isoformat(),
                }
                if random.random() < messiness:
                    row["os_version"] = None  # null messiness
                rows.append(row)

    # out-of-order change_timestamp (5%) — exercises unsorted CDC handling in SCD2
    n_shuffle = int(len(rows) * 0.05)
    for _ in range(n_shuffle):
        i, j = random.sample(range(len(rows)), 2)
        rows[i], rows[j] = rows[j], rows[i]

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate devices CDC events")
    parser.add_argument("--users", type=int, default=5000)
    parser.add_argument("--out", type=str, default="landing/devices_cdc")
    args = parser.parse_args()

    rows = generate_devices_cdc(n_users=args.users)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"devices_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} CDC events to {path}")


if __name__ == "__main__":
    main()
