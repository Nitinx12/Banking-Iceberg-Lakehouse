"""Generate profiles — household sub-profiles (conformed dimension, 1 user -> N profiles)."""

import argparse
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(47)
random.seed(47)

LANGUAGES = ["en", "es", "fr", "de", "hi"]
AVATARS = ["fox", "panda", "robot", "dino", "cat", "alien", "unicorn", "shark"]


def generate_profiles(
    n_users: int = 5000,
    messiness: float = 0.01,
) -> list[dict]:
    """One-to-many: each user gets 1-2 household profiles (~1.5x users).

    Messiness: invalid language code (1%), duplicate profile_id (0.5%).
    """
    rows: list[dict] = []
    base = datetime.now(tz=UTC) - timedelta(days=730)

    for i in range(n_users):
        user_id = f"user_{i:06d}"
        for _ in range(random.randint(1, 2)):
            row: dict = {
                "profile_id": f"prof_{uuid.uuid4().hex[:10]}",
                "user_id": user_id,
                "profile_name": fake.first_name(),
                "is_kids": random.random() < 0.2,
                "language": random.choice(LANGUAGES),
                "avatar": random.choice(AVATARS),
                "created_at": (
                    base + timedelta(days=random.randint(0, 700))
                ).isoformat(),
            }
            if random.random() < messiness:
                row["language"] = "xx"  # invalid language messiness
            rows.append(row)

    # duplicate profile_id (0.5%)
    for r in random.sample(rows, max(1, int(len(rows) * 0.005))):
        rows.append(r.copy())

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate profiles")
    parser.add_argument("--users", type=int, default=5000)
    parser.add_argument("--out", type=str, default="landing/profiles")
    args = parser.parse_args()

    rows = generate_profiles(n_users=args.users)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"profiles_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
