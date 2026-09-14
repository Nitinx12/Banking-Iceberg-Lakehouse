"""Central logger — respects LOG_LEVEL / DEBUG_MODE from .env / src/config.py."""

from __future__ import annotations

import logging
import sys


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

    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    if cfg.debug_mode:
        fmt = "%(asctime)s | %(levelname)-7s | %(name)s:%(lineno)d | %(message)s"
    handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False

    # Quiet noisy libs unless debug
    if not cfg.debug_mode:
        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.WARNING)
    return logger
