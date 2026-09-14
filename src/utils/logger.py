"""Central logger — rich-formatted, respects LOG_LEVEL / DEBUG_MODE from .env / src/config.py."""

from __future__ import annotations

import logging

from rich.logging import RichHandler

from src.utils.console import console


def get_logger(name: str = "streamflix") -> logging.Logger:
    """Return a configured logger. Idempotent — safe to call per module."""
    from src.config import get_config

    cfg = get_config()
    level_str = cfg.log_level.upper() if cfg.log_level else "INFO"
    level = getattr(logging, level_str, logging.INFO)
    if cfg.debug_mode:
        level = logging.DEBUG

    logger = logging.getLogger(name)
    if logger.handlers:
        logger.setLevel(level)
        return logger

    # RichHandler routes through the shared console, so log lines stack neatly
    # on top of any live progress display instead of breaking it.
    handler = RichHandler(
        console=console,
        show_time=True,
        show_path=cfg.debug_mode,
        rich_tracebacks=True,
        tracebacks_show_locals=False,
        markup=False,
    )
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False

    # Quiet noisy libs unless debug
    if not cfg.debug_mode:
        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.WARNING)
    return logger
