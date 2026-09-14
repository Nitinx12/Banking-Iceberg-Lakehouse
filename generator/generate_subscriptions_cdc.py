"""Generate subscriptions CDC change events (Debezium-style)."""

import argparse
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(43)
random.seed(43)

PLAN_TIERS = ["basic", "standard", "premium"]
STATUSES = ["active", "canceled", "paused"]
EVENT_TYPES = ["insert", "update", "delete"]


def generate_subscriptions_cdc(
    n_users: int = 5000,
    events_per_user: int = 3,
    messiness: float = 0.02,
) -> list[dict]:
    """Emit CDC events sorted randomly to simulate out-of-order delivery."""
    rows: list[dict] = []
    base = datetime.now(tz=UTC) - timedelta(days=365)

    for _ in range(n_users):
        user_id = f"user_{uuid.uuid4().hex[:8]}"
        subscription_id = f"sub_{uuid.uuid4().hex[:8]}"
        plan = random.choice(PLAN_TIERS)
        ts = base + timedelta(days=random.randint(0, 300))

        # initial insert
        rows.append(
            {
                "event_type": "insert",
                "subscription_id": subscription_id,
                "user_id": user_id,
                "plan_tier": plan,
                "status": "active",
                "change_timestamp": ts.isoformat(),
                "previous_plan_tier": None,
            }
        )

        prev_plan = plan
        for _ in range(random.randint(0, events_per_user - 1)):
            ts = ts + timedelta(
                days=random.randint(1, 60), seconds=random.randint(0, 86400)
            )
            new_plan = random.choice(PLAN_TIERS)
            event_type = random.choices(EVENT_TYPES, weights=[0.1, 0.8, 0.1])[0]
            status = random.choice(STATUSES) if event_type != "delete" else "canceled"
            row: dict = {
                "event_type": event_type,
                "subscription_id": subscription_id,
                "user_id": user_id,
                "plan_tier": new_plan,
                "status": status,
                "change_timestamp": ts.isoformat(),
                "previous_plan_tier": prev_plan,
            }
            if event_type == "delete":
                row["plan_tier"] = prev_plan  # delete retains last plan
            if random.random() < messiness:
                row["previous_plan_tier"] = None  # null messiness
            rows.append(row)
            prev_plan = new_plan

    # out-of-order timestamps (shuffle within small window to keep overall trend)
    # intentional: shuffle 5% of rows to exercise unsorted change_timestamp handling in SCD2
    n_shuffle = int(len(rows) * 0.05)
    for _ in range(n_shuffle):
        i, j = random.sample(range(len(rows)), 2)
        rows[i], rows[j] = rows[j], rows[i]

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate subscriptions CDC events")
    parser.add_argument("--users", type=int, default=5000)
    parser.add_argument("--out", type=str, default="landing/subscriptions_cdc")
    args = parser.parse_args()

    rows = generate_subscriptions_cdc(n_users=args.users)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"subscriptions_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} CDC events to {path}")


if __name__ == "__main__":
    main()
