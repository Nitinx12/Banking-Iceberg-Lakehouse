"""Generate promotion_redemptions — factless fact / bridge table (user <-> promo many-to-many)."""

import argparse
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(49)
random.seed(49)


def generate_promotion_redemptions(
    n: int = 8000,
    n_promos: int = 200,
    n_users: int = 5000,
    messiness: float = 0.01,
) -> list[dict]:
    """Bridge rows linking users to promo codes at a billing period.

    Messiness: duplicate redemption_id (0.5%), orphaned promo_code (1%),
    future redeemed_at (0.1%).
    """
    user_ids = [f"user_{i:06d}" for i in range(n_users)]
    # same deterministic code space as generate_promotions (PROMO26_000000...) —
    # n_promos must match the promotions row count so FKs resolve; only the
    # deliberate "PROMO26_DOESNOTEXIST" messiness orphans
    promo_codes = [f"PROMO26_{i:06X}" for i in range(n_promos)]
    base = datetime.now(tz=UTC) - timedelta(days=180)

    rows: list[dict] = []
    for _ in range(n):
        redeemed_at = base + timedelta(seconds=random.randint(0, 180 * 86400))
        # billing period = month start of redemption
        period_start = redeemed_at.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        row: dict = {
            "redemption_id": f"red_{uuid.uuid4().hex[:12]}",
            "promo_code": random.choice(promo_codes),
            "user_id": random.choice(user_ids),
            "subscription_id": f"sub_{uuid.uuid4().hex[:8]}",
            "redeemed_at": redeemed_at.isoformat(),
            "billing_period_start": period_start.date().isoformat(),
        }
        if random.random() < messiness:
            row["promo_code"] = "PROMO26_DOESNOTEXIST"  # orphaned FK messiness
        if random.random() < 0.001:
            row["redeemed_at"] = (
                redeemed_at + timedelta(days=30)
            ).isoformat()  # future
        rows.append(row)

    # duplicate redemption_id (0.5%)
    for r in random.sample(rows, max(1, int(len(rows) * 0.005))):
        rows.append(r.copy())

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate promotion_redemptions")
    parser.add_argument("--rows", type=int, default=8000)
    parser.add_argument("--out", type=str, default="landing/promotion_redemptions")
    args = parser.parse_args()

    rows = generate_promotion_redemptions(n=args.rows)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"promotion_redemptions_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
