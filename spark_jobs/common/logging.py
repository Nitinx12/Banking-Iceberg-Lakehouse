"""spark_jobs/common/logging.py — stage-scoped structured logging (Architecture 12.1)."""

import logging
import os
import pathlib

LOG_DIR = pathlib.Path(os.getenv("LOG_DIR", "logs"))


def get_logger(stage: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(stage)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(LOG_DIR / f"{stage}.log")
    sh = logging.StreamHandler()
    fmt = logging.Formatter(
        '{"ts":"%(asctime)s","stage":"' + stage + '","level":"%(levelname)s","msg":"%(message)s"}'
    )
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger
