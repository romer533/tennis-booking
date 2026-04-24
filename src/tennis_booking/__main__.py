"""Service entry point: `python -m tennis_booking`.

Lifecycle:
  1. argparse → config_dir / log_level / dry_run
  2. setup_logging(log_dir)
  3. AltegioConfig.from_env() — fails fast if ALTEGIO_BEARER_TOKEN missing
  4. load_app_config(config_dir) — fails fast on bad schedule/profiles
  5. SchedulerLoop(...).run() inside `async with AltegioClient(...)`
  6. SIGTERM / SIGINT → loop.stop() (graceful shutdown, no async-kills mid-fire)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

from tennis_booking.altegio.client import AltegioClient
from tennis_booking.altegio.config import AltegioConfig
from tennis_booking.altegio.errors import AltegioConfigError
from tennis_booking.common.clock import SystemClock
from tennis_booking.config.errors import ConfigError
from tennis_booking.config.loader import load_app_config
from tennis_booking.obs import setup_logging
from tennis_booking.scheduler.loop import SchedulerLoop

DEFAULT_CONFIG_DIR = Path("/etc/tennis-booking")
DEFAULT_LOG_DIR = Path("/var/log/tennis-booking")
DEFAULT_LOG_LEVEL = "INFO"

EXIT_OK = 0
EXIT_ERROR = 1


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tennis-booking",
        description="Automated tennis court booking against Altegio.",
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=DEFAULT_CONFIG_DIR,
        help=(
            f"Directory containing schedule.yaml and profiles.yaml "
            f"(default: {DEFAULT_CONFIG_DIR})."
        ),
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=DEFAULT_LOG_LEVEL,
        help=f"DEBUG/INFO/WARNING/ERROR (default: {DEFAULT_LOG_LEVEL}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Do not POST bookings to Altegio. "
            "Equivalent to env ALTEGIO_DRY_RUN=1; CLI flag wins if set."
        ),
    )
    return parser.parse_args(argv)


def _resolve_log_dir() -> Path:
    raw = os.environ.get("TENNIS_LOG_DIR")
    if raw and raw.strip():
        return Path(raw.strip())
    return DEFAULT_LOG_DIR


def _parse_ntp_required(env_value: str | None) -> bool:
    """TENNIS_NTP_REQUIRED escape hatch for dev environments without NTP.

    Default is True (production) — only the explicit falsy strings below
    disable the startup NTP fail-fast. Anything else (including unrecognised
    values like "yes" or "1") is treated as truthy so a typo cannot silently
    weaken production posture.
    """
    if env_value is None:
        return True
    normalized = env_value.strip().lower()
    return normalized not in ("0", "false", "no", "off", "")


def _install_signal_handlers(
    event_loop: asyncio.AbstractEventLoop,
    scheduler_loop: SchedulerLoop,
    logger: logging.Logger,
) -> None:
    """Wire SIGTERM/SIGINT → scheduler_loop.stop().

    On Windows `add_signal_handler` raises NotImplementedError for SIGTERM (and
    sometimes SIGINT). Fall back to KeyboardInterrupt there — this is a Linux-first
    service, Windows is dev-only.
    """

    def _request_stop(sig_name: str) -> None:
        logger.info("signal_received: %s → stop()", sig_name)
        event_loop.create_task(scheduler_loop.stop())

    for sig, sig_name in ((signal.SIGTERM, "SIGTERM"), (signal.SIGINT, "SIGINT")):
        try:
            event_loop.add_signal_handler(sig, _request_stop, sig_name)
        except NotImplementedError:
            # Windows: SIGTERM not supported by ProactorEventLoop. SIGINT falls
            # through to the default handler → KeyboardInterrupt at `run()`.
            logger.debug("signal_handler_unavailable: %s (likely Windows)", sig_name)


async def _run(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        altegio_config = AltegioConfig.from_env()
    except AltegioConfigError as e:
        logger.error("altegio_config_error: %s", e)
        print(
            "ERROR: Altegio config invalid. "
            "Set ALTEGIO_BEARER_TOKEN in environment (or EnvironmentFile for systemd). "
            f"Cause: {e}",
            file=sys.stderr,
        )
        return EXIT_ERROR

    if args.dry_run and not altegio_config.dry_run:
        altegio_config = altegio_config.model_copy(update={"dry_run": True})

    try:
        app_config = load_app_config(args.config_dir)
    except ConfigError as e:
        logger.error("app_config_error: %s", e)
        print(f"ERROR: config invalid: {e}", file=sys.stderr)
        return EXIT_ERROR

    logger.info(
        "starting: config_dir=%s bookings=%d profiles=%d dry_run=%s",
        args.config_dir,
        len(app_config.bookings),
        len(app_config.profiles),
        altegio_config.dry_run,
    )

    ntp_required = _parse_ntp_required(os.environ.get("TENNIS_NTP_REQUIRED"))
    if not ntp_required:
        logger.warning(
            "ntp_required=False (TENNIS_NTP_REQUIRED env override) "
            "— will not fail-fast on NTP errors"
        )

    async with AltegioClient(altegio_config) as client:
        scheduler_loop = SchedulerLoop(
            app_config,
            client,
            SystemClock(),
            ntp_required=ntp_required,
        )
        event_loop = asyncio.get_running_loop()
        _install_signal_handlers(event_loop, scheduler_loop, logger)

        try:
            await scheduler_loop.run()
        except KeyboardInterrupt:
            logger.info("keyboard_interrupt → stopping")
            await scheduler_loop.stop()
        except Exception as e:  # noqa: BLE001 — top-level; log and surface non-zero exit
            logger.exception("loop_crashed: %s", e)
            return EXIT_ERROR

    logger.info("exited cleanly")
    return EXIT_OK


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    log_dir = _resolve_log_dir()

    try:
        setup_logging(log_dir, log_level=args.log_level)
    except (OSError, ValueError) as e:
        print(f"ERROR: cannot set up logging at {log_dir}: {e}", file=sys.stderr)
        return EXIT_ERROR

    logger = logging.getLogger("tennis_booking.main")
    return await _run(args, logger)


def entrypoint() -> None:
    try:
        code = asyncio.run(main())
    except KeyboardInterrupt:
        # asyncio.run re-raises KeyboardInterrupt if Ctrl-C lands before loop setup.
        code = EXIT_OK
    sys.exit(code)


if __name__ == "__main__":
    entrypoint()
