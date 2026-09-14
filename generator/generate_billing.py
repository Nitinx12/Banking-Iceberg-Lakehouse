"""Generate billing_transactions."""

import argparse
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(45)
random.seed(45)

PLAN_PRICES = {"basic": 9.99, "standard": 15.99, "premium": 19.99}
CURRENCIES = ["USD", "EUR", "GBP"]
TX_TYPES = ["charge", "refund"]


def generate_billing(
    n: int = 20_000, n_users: int = 5000, messiness: float = 0.01
) -> list[dict]:
    user_ids = [f"user_{i:06d}" for i in range(n_users)]
    base = datetime.now(tz=UTC) - timedelta(days=90)
    rows: list[dict] = []
    for _ in range(n):
        plan = random.choice(list(PLAN_PRICES.keys()))
        amount = PLAN_PRICES[plan]
        tx_type = random.choices(TX_TYPES, weights=[0.95, 0.05])[0]
        if tx_type == "refund":
            amount = -amount
        # small jitter
        amount = round(amount + random.uniform(-1, 1), 2)
        ts = base + timedelta(seconds=random.randint(0, 90 * 86400))
        row: dict = {
            "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
            "user_id": random.choice(user_ids),
            "amount": amount,
            "currency": random.choice(CURRENCIES),
            "transaction_type": tx_type,
            "transaction_timestamp": ts.isoformat(),
            "plan_tier": plan,
        }
        if random.random() < messiness:
            row["currency"] = None
        rows.append(row)

    # duplicates 0.2%
    for r in random.sample(rows, int(len(rows) * 0.002)):
        rows.append(r.copy())

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate billing_transactions")
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--out", type=str, default="landing/billing")
    args = parser.parse_args()

    rows = generate_billing(n=args.rows)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"billing_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
