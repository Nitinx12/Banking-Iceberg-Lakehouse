"""Smoke tests — run the push-CI steps locally so green here means green on push.

Mirrors ci.yml (ruff, test collection) and the nightly E2E's generate + GX
steps in safe, in-process form: generators are invoked directly with tiny
counts, so landing/ is never touched.
"""

import json
import subprocess
import sys
from pathlib import Path

from generator.generate_billing import generate_billing
from generator.generate_cdn_stream_logs import generate_cdn_stream_logs
from generator.generate_content_catalog import generate_content_catalog
from generator.generate_content_ratings import generate_content_ratings
from generator.generate_devices_cdc import generate_devices_cdc
from generator.generate_profiles import generate_profiles
from generator.generate_promotion_redemptions import generate_promotion_redemptions
from generator.generate_promotions import generate_promotions
from generator.generate_subscriptions_cdc import generate_subscriptions_cdc
from generator.generate_support_tickets import generate_support_tickets
from generator.generate_watch_events import generate_watch_events

REPO_ROOT = Path(__file__).resolve().parents[1]

N_USERS, N_CONTENT, N_PROMOS = 20, 10, 10

# (source, pk column, generator call) — tiny counts, same call shapes as cmd_generate
GENERATOR_CASES = [
    ("content_catalog", "content_id", lambda: generate_content_catalog(n=N_CONTENT)),
    (
        "subscriptions_cdc",
        "subscription_id",
        lambda: generate_subscriptions_cdc(n_users=N_USERS),
    ),
    (
        "watch_events",
        "event_id",
        lambda: generate_watch_events(n=100, n_users=N_USERS, n_content=N_CONTENT),
    ),
    ("billing", "transaction_id", lambda: generate_billing(n=50, n_users=N_USERS)),
    ("devices_cdc", "device_id", lambda: generate_devices_cdc(n_users=N_USERS)),
    ("profiles", "profile_id", lambda: generate_profiles(n_users=N_USERS)),
    ("promotions", "promo_code", lambda: generate_promotions(n=N_PROMOS)),
    (
        "promotion_redemptions",
        "redemption_id",
        lambda: generate_promotion_redemptions(
            n=50, n_promos=N_PROMOS, n_users=N_USERS
        ),
    ),
    (
        "support_tickets",
        "ticket_id",
        lambda: generate_support_tickets(n=30, n_users=N_USERS),
    ),
    (
        "cdn_stream_logs",
        "log_id",
        lambda: generate_cdn_stream_logs(n=100, n_users=N_USERS, n_content=N_CONTENT),
    ),
    (
        "content_ratings",
        "rating_id",
        lambda: generate_content_ratings(n=40, n_users=N_USERS, n_content=N_CONTENT),
    ),
]


def _run(cmd: list[str], timeout: int = 300) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def test_ruff_check_passes():
    """ci.yml step: `uv run ruff check .` — same command, same venv."""
    res = _run([sys.executable, "-m", "ruff", "check", "."])
    assert res.returncode == 0, f"ruff failed:\n{res.stdout}\n{res.stderr}"


def test_pytest_collects_cleanly():
    """ci.yml step: `uv run pytest` — collection from a cold interpreter."""
    res = _run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "tests"],
        timeout=300,
    )
    assert res.returncode == 0, f"collection failed:\n{res.stdout[-2000:]}"


def test_all_generators_produce_rows_with_keys():
    """Nightly E2E step: `main.py generate` — every source yields rows with its PK."""
    for name, pk, generate in GENERATOR_CASES:
        rows = generate()
        assert rows, f"{name}: generator returned no rows"
        for row in rows[:5]:
            assert row.get(pk), f"{name}: row missing PK {pk!r}: {row}"


def test_promo_code_space_is_shared():
    """promotions and promotion_redemptions must draw from one deterministic
    code space — this is what makes the bridge table's FK quarantine meaningful."""
    codes = {r["promo_code"] for r in generate_promotions(n=N_PROMOS)}
    redemptions = generate_promotion_redemptions(
        n=50, n_promos=N_PROMOS, n_users=N_USERS
    )
    used = {r["promo_code"] for r in redemptions} - {"PROMO26_DOESNOTEXIST"}
    assert used <= codes, (
        f"redemptions reference promo codes outside the promotions space: "
        f"{sorted(used - codes)}"
    )


def test_user_id_space_is_shared():
    """Facts must draw user ids from the same range as the subscription dim."""
    users = {r["user_id"] for r in generate_subscriptions_cdc(n_users=N_USERS)}
    events = generate_watch_events(n=100, n_users=N_USERS, n_content=N_CONTENT)
    used = {r["user_id"] for r in events}
    assert used <= users, (
        f"watch_events reference unknown users: {sorted(used - users)}"
    )


def test_all_gx_suites_resolve():
    """Nightly E2E step: `main.py gx --list` — all 11 suites parse with content."""
    files = sorted((REPO_ROOT / "gx" / "expectations").glob("*.json"))
    assert len(files) == 11, f"expected 11 GX suites, found {len(files)}"
    for path in files:
        suite = json.loads(path.read_text(encoding="utf-8"))
        assert suite.get("expectation_suite_name"), f"{path.name}: missing suite name"
        expectations = suite.get("expectations", [])
        assert expectations, f"{path.name}: no expectations"
        for exp in expectations:
            assert exp.get("expectation_type"), (
                f"{path.name}: expectation without a type"
            )


def test_main_cli_parses_core_commands():
    """The CLI surface CI and the CE Job depend on must keep parsing."""
    from main import build_parser

    for argv in (
        ["pipeline"],
        ["push"],
        ["generate", "--users", "5", "--watch", "10"],
        ["bronze", "--what", "billing"],
        ["silver", "--what", "devices_scd2"],
        ["gold", "--what", "all"],
        ["test-connection"],
        ["gx", "--list"],
    ):
        args = build_parser().parse_args(argv)
        assert hasattr(args, "func"), f"argv {argv} did not resolve to a command"
