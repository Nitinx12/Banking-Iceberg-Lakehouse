"""jobs/common/metrics.py — push pipeline metrics to Prometheus Pushgateway (Architecture 12.1, 13.4).

Best-effort by design: monitoring is optional infrastructure, so a missing
pushgateway must never fail a data run. Callers pass {metric_name: (labels, value)};
labels are dicts like {"table": "fct_transactions"}.
"""

import os

from dotenv import load_dotenv

from jobs.common.logging import get_logger

load_dotenv()

logger = get_logger("metrics")

# fail-fast for the rest of the process: first network failure disables all later pushes
_PUSHGATEWAY_AVAILABLE: bool | None = None


def _is_metrics_disabled() -> bool:
    # read env lazily so .env loaded by jobs/common/config.py is respected
    # and so `METRICS_ENABLED=false` or empty `PUSHGATEWAY_URL` disables without a network call
    metrics_enabled = os.getenv("METRICS_ENABLED", "").lower() not in ("0", "false", "no", "off")
    # empty means disabled; default "" keeps dev quiet, prod sets it explicitly
    pushgateway_url = os.getenv("PUSHGATEWAY_URL", "").strip()
    if not metrics_enabled:
        return True
    if not pushgateway_url:
        return True
    if _PUSHGATEWAY_AVAILABLE is False:
        return True
    return False


def _pushgateway_url() -> str:
    return os.getenv("PUSHGATEWAY_URL", "").strip()


def push_metrics(
    job: str,
    metrics: dict[str, tuple[dict[str, str], float] | list[tuple[dict[str, str], float]]],
    run_id: str = "",
) -> bool:
    """Push metrics to the Pushgateway. Returns success (never raises).

    Each metric name maps to one (labels, value) pair or a list of them (multiple
    series of the same name, e.g. data_freshness_seconds per table). Series arrive
    in Prometheus as (job, run_id, <labels>) -> value, e.g.
    data_freshness_seconds{job="sla_monitor", table="fct_transactions"}.
    """
    if not metrics:
        return False
    if _is_metrics_disabled():
        return False
    lines = []
    for name, entries in metrics.items():
        series = entries if isinstance(entries, list) else [entries]
        lines.append(f"# TYPE {name} gauge")
        for labels, value in series:
            label_parts = [f'{k}="{v}"' for k, v in labels.items()]
            if run_id:
                label_parts.append(f'run_id="{run_id}"')
            label_str = "{" + ",".join(label_parts) + "}" if label_parts else ""
            lines.append(f"{name}{label_str} {value}")
    body = "\n".join(lines) + "\n"
    global _PUSHGATEWAY_AVAILABLE  # noqa: PLW0603
    try:
        from urllib.request import Request, urlopen

        req = Request(
            f"{_pushgateway_url()}/metrics/job/{job}",
            data=body.encode(),
            headers={"Content-Type": "text/plain"},
            method="POST",
        )
        with urlopen(req, timeout=2):
            pass
        # success re-enables (in case it was previously disabled)
        _PUSHGATEWAY_AVAILABLE = True
        logger.info(f"pushed {len(metrics)} metrics to pushgateway job={job}")
        return True
    except Exception as e:  # noqa: BLE001 — monitoring must never fail a data run
        # first failure disables the rest of the run — no more 5s timeouts per stage
        first_failure = _PUSHGATEWAY_AVAILABLE is None
        _PUSHGATEWAY_AVAILABLE = False
        if first_failure:
            logger.warning(f"pushgateway unavailable ({e}) — metrics disabled for this run")
        return False
