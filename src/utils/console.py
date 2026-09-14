"""Rich console — one shared console for logs, progress bars, and CLI output.

Also owns terminal hygiene: pinning the Spark worker python (avoids the Windows
Store `python` alias) and suppressing OS-level fd noise the JVM emits at startup
(log4j banners, `jdk.incubator.vector` warning) and at shutdown (taskkill).
"""

from __future__ import annotations

import contextlib
import os
import sys
import tempfile
import warnings

from rich.console import Console

# Every logger / Progress / CLI print goes through this one console so rich can
# keep the live progress display intact when log lines interleave.
console = Console()


def setup_clean_output() -> None:
    """Call once at CLI startup — cheap, idempotent."""
    # Spark workers: use the venv interpreter, not the Windows Store alias
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # pyspark.testing.utils FutureWarning about pandas >= 3
    warnings.filterwarnings(
        "ignore",
        message="PySpark does not yet fully support pandas",
        category=FutureWarning,
    )


@contextlib.contextmanager
def quiet_fds(*fds: int):
    """Redirect OS-level fds into a buffer; restore on exit.

    The JVM writes its startup banners straight to fd 2 and pyspark's Windows
    shutdown taskkill writes to fd 1/2 — neither goes through Python, so only a
    fd-level dup2 can capture them. On exception, re-raise happens *after* the
    fds are restored, so tracebacks still print normally.
    """
    fds = fds or (1, 2)
    saved = {}
    with tempfile.TemporaryFile(mode="w+b") as buf:
        try:
            for fd in fds:
                saved[fd] = os.dup(fd)
                os.dup2(buf.fileno(), fd)
            yield buf
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            for fd, old in saved.items():
                os.dup2(old, fd)
                os.close(old)


def show_captured(buf) -> None:
    """Dump captured fd noise to stderr — for when a suppressed step failed."""
    buf.flush()
    buf.seek(0)
    sys.stderr.buffer.write(buf.read())


def mute_fds_at_exit() -> None:
    """Send fd 1/2 to the null device for the rest of the process.

    pyspark's at-exit shutdown on Windows shells out to `taskkill`, whose
    "SUCCESS: The process with PID ..." chatter lands after every run. Call
    this *after* the Spark session exists: atexit runs LIFO, so this handler
    fires before pyspark's own, and nothing legitimate prints that late anyway.
    """
    import atexit

    def _mute():
        devnull = os.open(os.devnull, os.O_WRONLY)
        for fd in (1, 2):
            try:
                os.dup2(devnull, fd)
            except OSError:
                pass

    atexit.register(_mute)
