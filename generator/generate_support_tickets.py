"""Generate support_tickets — semi-structured: nested JSON payload parsed in Silver via from_json."""

import argparse
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(50)
random.seed(50)

STATUSES = ["open", "pending", "resolved", "closed"]
CHANNELS = ["chat", "email", "phone"]
CATEGORIES = ["playback", "billing", "account", "content", "connectivity"]
SUBCATEGORIES = {
    "playback": ["buffering", "subtitle_sync", "quality_drop"],
    "billing": ["duplicate_charge", "refund", "plan_change"],
    "account": ["login", "password_reset", "profile_setup"],
    "content": ["missing_episode", "wrong_metadata", "request_content"],
    "connectivity": ["app_crash", "offline_mode", "device_pairing"],
}
DEVICE_TYPES = ["tv", "mobile", "web", "tablet", "console"]
OS_FAMILIES = ["android", "ios", "tvos", "windows", "webos", "tizen"]


def _make_payload() -> dict:
    category = random.choice(CATEGORIES)
    return {
        "category": category,
        "subcategory": random.choice(SUBCATEGORIES[category]),
        "device": {
            "device_type": random.choice(DEVICE_TYPES),
            "os_family": random.choice(OS_FAMILIES),
        },
        "resolution_notes": fake.sentence(nb_words=8),
    }


def generate_support_tickets(
    n: int = 5000, n_users: int = 5000, messiness: float = 0.01
) -> list[dict]:
    """Top-level columns + nested JSON payload string.

    Messiness: malformed payload JSON (1%), csat out of range (0.5%),
    duplicate ticket_id (0.2%).
    """
    user_ids = [f"user_{i:06d}" for i in range(n_users)]
    base = datetime.now(tz=UTC) - timedelta(days=180)

    rows: list[dict] = []
    for _ in range(n):
        created_at = base + timedelta(seconds=random.randint(0, 180 * 86400))
        updated_at = created_at + timedelta(hours=random.randint(1, 72))
        status = random.choices(STATUSES, weights=[0.2, 0.15, 0.45, 0.2])[0]
        # csat only collected once resolved/closed
        csat = (
            random.randint(1, 5)
            if status in ("resolved", "closed") and random.random() < 0.7
            else None
        )
        payload = json.dumps(_make_payload())
        if random.random() < messiness:
            payload = payload[: max(1, len(payload) // 2)]  # malformed JSON messiness
        row: dict = {
            "ticket_id": f"tkt_{uuid.uuid4().hex[:12]}",
            "user_id": random.choice(user_ids),
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
            "status": status,
            "channel": random.choice(CHANNELS),
            "csat_score": csat,
            "payload": payload,
        }
        if random.random() < 0.005:
            row["csat_score"] = random.choice([0, 9])  # out of range
        rows.append(row)

    # duplicate ticket_id (0.2%)
    for r in random.sample(rows, max(1, int(len(rows) * 0.002))):
        rows.append(r.copy())

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate support_tickets")
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--out", type=str, default="landing/support_tickets")
    args = parser.parse_args()

    rows = generate_support_tickets(n=args.rows)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"support_tickets_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
