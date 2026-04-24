"""Logging setup: rotating file handler + stdout, structlog → stdlib bridge.

Two sinks live in parallel:
  - RotatingFileHandler({log_dir}/service.log) — Python-managed rotation, survives
    systemd restarts, grep-friendly. Pattern service.log, service.log.1, ..., service.log.14.
  - StreamHandler(stdout) — picked up by systemd journal (StandardOutput=journal).

Both emit the same JSON line. Bearer-token redaction filter installed on root —
keeps us from accidentally leaking tokens via any logger (including httpx/httpcore
traces that bypass the altegio/client.py filter if they're attached after import).
"""
from __future__ import annotations

import logging
import logging.handlers
import re
import sys
from pathlib import Path

import structlog

__all__ = [
    "LOG_FILENAME",
    "LOG_MAX_BYTES",
    "LOG_BACKUP_COUNT",
    "setup_logging",
]

LOG_FILENAME = "service.log"
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MiB
LOG_BACKUP_COUNT = 14  # → service.log.1 .. service.log.14; older deleted by handler

_BEARER_RE = re.compile(r"(Bearer\s+)([^\s'\"]+)", re.IGNORECASE)


class _BearerRedactFilter(logging.Filter):
    """Strips Bearer tokens from any formatted log record. Safety net."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001 — never break logging on format bugs
            return True
        redacted = _BEARER_RE.sub(r"\1***", msg)
        if redacted != msg:
            record.msg = redacted
            record.args = None
        return True


def _resolve_log_level(log_level: str) -> int:
    level = logging.getLevelName(log_level.upper())
    if not isinstance(level, int):
        raise ValueError(
            f"invalid log_level {log_level!r} "
            f"(expected DEBUG/INFO/WARNING/ERROR/CRITICAL)"
        )
    return level


def setup_logging(log_dir: Path, log_level: str = "INFO") -> None:
    """Configure stdlib logging root + structlog.

    Idempotent: re-running replaces handlers on the root logger (tests / reload).

    Args:
        log_dir: directory for service.log. Created if missing.
        log_level: DEBUG/INFO/WARNING/ERROR/CRITICAL (case-insensitive).

    Raises:
        ValueError: invalid log_level.
        OSError: cannot create log_dir (e.g. permission denied).
    """
    level = _resolve_log_level(log_level)

    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / LOG_FILENAME

    root = logging.getLogger()
    # Drop any handlers from a prior setup — otherwise tests / re-calls duplicate output.
    for h in list(root.handlers):
        root.removeHandler(h)
        try:
            h.close()
        except Exception:  # noqa: BLE001
            pass

    formatter = logging.Formatter("%(message)s")

    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(_BearerRedactFilter())

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(_BearerRedactFilter())

    root.setLevel(level)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)

    # structlog → stdlib bridge: structlog processors render JSON, then a single
    # %(message)s formatter passes it through unchanged.
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
