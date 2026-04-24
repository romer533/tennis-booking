from __future__ import annotations

import json
import logging
import logging.handlers
from collections.abc import Iterator
from pathlib import Path

import pytest

from tennis_booking.obs.logging import (
    LOG_BACKUP_COUNT,
    LOG_FILENAME,
    LOG_MAX_BYTES,
    setup_logging,
)


@pytest.fixture(autouse=True)
def _reset_logging_after_test() -> Iterator[None]:
    """Each test must leave the root logger in its original state."""
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
        try:
            h.close()
        except Exception:  # noqa: BLE001
            pass
    for h in original_handlers:
        root.addHandler(h)
    root.setLevel(original_level)


def test_creates_log_dir_and_file(tmp_path: Path) -> None:
    log_dir = tmp_path / "does" / "not" / "exist"
    setup_logging(log_dir, log_level="INFO")

    logging.getLogger("test").info("hello")
    # Force flush
    for h in logging.getLogger().handlers:
        h.flush()

    assert log_dir.is_dir()
    assert (log_dir / LOG_FILENAME).exists()


def test_rotating_handler_config(tmp_path: Path) -> None:
    setup_logging(tmp_path, log_level="INFO")

    rotating = [
        h
        for h in logging.getLogger().handlers
        if isinstance(h, logging.handlers.RotatingFileHandler)
    ]
    assert len(rotating) == 1
    handler = rotating[0]
    assert handler.maxBytes == LOG_MAX_BYTES
    assert handler.backupCount == LOG_BACKUP_COUNT
    assert Path(handler.baseFilename).name == LOG_FILENAME
    assert LOG_MAX_BYTES == 10 * 1024 * 1024
    assert LOG_BACKUP_COUNT == 14


def test_stdout_stream_handler_present(tmp_path: Path) -> None:
    setup_logging(tmp_path, log_level="INFO")
    stream = [
        h
        for h in logging.getLogger().handlers
        if type(h) is logging.StreamHandler
    ]
    assert len(stream) == 1


def test_invalid_log_level_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="invalid log_level"):
        setup_logging(tmp_path, log_level="BOGUS")


def test_rotation_occurs_on_size_exceed(tmp_path: Path) -> None:
    """Write until the handler rotates; service.log.1 must materialize."""
    setup_logging(tmp_path, log_level="INFO")

    log_file = tmp_path / LOG_FILENAME
    rotated_1 = tmp_path / f"{LOG_FILENAME}.1"

    logger = logging.getLogger("rotation_test")
    payload = "x" * 1024  # 1 KiB
    # 10 MiB / 1 KiB + overhead → ~10500 lines is plenty to cross the boundary.
    for _ in range(12000):
        logger.info(payload)
        if rotated_1.exists():
            break
    for h in logging.getLogger().handlers:
        h.flush()

    assert rotated_1.exists(), "RotatingFileHandler did not roll over at maxBytes"
    assert log_file.exists(), "active log file missing after rotation"


def test_json_output_format(tmp_path: Path) -> None:
    """structlog writes one JSON object per line."""
    import structlog

    setup_logging(tmp_path, log_level="INFO")

    log = structlog.get_logger("json_test")
    log.info("event_name", extra_field=42, booking="courts_1")

    for h in logging.getLogger().handlers:
        h.flush()

    content = (tmp_path / LOG_FILENAME).read_text(encoding="utf-8").strip()
    assert content, "no log content written"
    last_line = content.splitlines()[-1]
    parsed = json.loads(last_line)
    assert parsed["event"] == "event_name"
    assert parsed["extra_field"] == 42
    assert parsed["booking"] == "courts_1"
    assert parsed["level"] == "info"
    assert "timestamp" in parsed


def test_bearer_token_redacted(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    setup_logging(tmp_path, log_level="DEBUG")

    logger = logging.getLogger("redact_test")
    logger.info("auth header: Bearer eyJhbGciOiJIUzI1NiJ9.secret")

    for h in logging.getLogger().handlers:
        h.flush()

    content = (tmp_path / LOG_FILENAME).read_text(encoding="utf-8")
    assert "eyJhbGciOiJIUzI1NiJ9.secret" not in content
    assert "Bearer ***" in content


def test_idempotent_reconfigure(tmp_path: Path) -> None:
    """Calling setup_logging twice must replace handlers, not accumulate."""
    setup_logging(tmp_path, log_level="INFO")
    first_count = len(logging.getLogger().handlers)

    setup_logging(tmp_path, log_level="DEBUG")
    second_count = len(logging.getLogger().handlers)

    assert first_count == second_count == 2  # file + stream
    assert logging.getLogger().level == logging.DEBUG
