"""Generate promotions — reference dimension with validity windows and redemption caps."""

import argparse
import json
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(48)
random.seed(48)

PLAN_TIERS = ["basic", "standard", "premium", None]  # None = all tiers eligible


def generate_promotions(n: int = 200, messiness: float = 0.005) -> list[dict]:
    """Small dimension: promo codes with discount_pct and start/end validity window.

    Messiness: end_before_start (0.5%), discount_pct out of range (0.5%).
    """
    now = datetime.now(tz=UTC)
    rows: list[dict] = []
    for i in range(n):
        starts_at = now - timedelta(days=random.randint(0, 300))
        ends_at = starts_at + timedelta(days=random.randint(7, 180))
        discount = random.choice([5, 10, 15, 20, 25, 30, 50])
        row: dict = {
            # deterministic, shared code space — generate_promotion_redemptions
            # derives its codes the same way so the bridge table has intact FKs
            "promo_code": f"PROMO26_{i:06X}",
            "description": fake.catch_phrase(),
            "discount_pct": discount,
            "starts_at": starts_at.isoformat(),
            "ends_at": ends_at.isoformat(),
            "eligible_plan_tier": random.choice(PLAN_TIERS),
            "max_redemptions": random.choice([100, 500, 1000, 5000, 10000]),
            "is_active": random.random() < 0.9,
        }
        if random.random() < messiness:
            row["ends_at"] = (
                starts_at - timedelta(days=1)
            ).isoformat()  # end before start
        if random.random() < messiness:
            row["discount_pct"] = random.choice([-10, 150])  # out of range
        rows.append(row)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate promotions")
    parser.add_argument("--rows", type=int, default=200)
    parser.add_argument("--out", type=str, default="landing/promotions")
    args = parser.parse_args()

    rows = generate_promotions(n=args.rows)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"promotions_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
