"""jobs/common/metrics.py — push pipeline metrics to Prometheus Pushgateway (Architecture 12.1, 13.4).

Best-effort by design: monitoring is optional infrastructure, so a missing
pushgateway must never fail a data run. Callers pass {metric_name: (labels, value)};
labels are dicts like {"table": "fct_transactions"}.
"""

import os

from jobs.common.logging import get_logger

logger = get_logger("metrics")

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")


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
    try:
        from urllib.request import Request, urlopen

        req = Request(
            f"{PUSHGATEWAY_URL}/metrics/job/{job}",
            data=body.encode(),
            headers={"Content-Type": "text/plain"},
            method="POST",
        )
        with urlopen(req, timeout=5):
            pass
        logger.info(f"pushed {len(metrics)} metrics to pushgateway job={job}")
        return True
    except Exception as e:  # noqa: BLE001 — monitoring must never fail a data run
        logger.warning(f"pushgateway unavailable ({e}) — metrics skipped")
        return False
