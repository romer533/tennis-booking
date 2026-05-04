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
from tennis_booking.obs.telegram import TelegramNotifier, disabled_notifier
from tennis_booking.persistence.cli import add_import_record_subparser, run_import_record
from tennis_booking.persistence.store import FileBookingStore
from tennis_booking.scheduler.loop import SchedulerLoop

DEFAULT_CONFIG_DIR = Path("/etc/tennis-booking")
DEFAULT_LOG_DIR = Path("/var/log/tennis-booking")
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_BOOKINGS_FILE = Path("/app/data/bookings.jsonl")
# In-code default is 0.0 (guard disabled) — production sets this to 2.0 via
# TENNIS_MIN_LEAD_TIME_HOURS in the systemd EnvironmentFile.
DEFAULT_MIN_LEAD_TIME_HOURS = 0.0
MAX_MIN_LEAD_TIME_HOURS = 168.0

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
    subparsers = parser.add_subparsers(dest="subcommand")
    add_import_record_subparser(subparsers)
    return parser.parse_args(argv)


def _resolve_store_path() -> Path:
    raw = os.environ.get("TENNIS_BOOKINGS_FILE")
    if raw and raw.strip():
        return Path(raw.strip())
    return DEFAULT_BOOKINGS_FILE


def _resolve_log_dir() -> Path:
    raw = os.environ.get("TENNIS_LOG_DIR")
    if raw and raw.strip():
        return Path(raw.strip())
    return DEFAULT_LOG_DIR


def _parse_min_lead_time_hours(env_value: str | None) -> float:
    """TENNIS_MIN_LEAD_TIME_HOURS — fail-fast at startup on invalid values.

    Empty / unset → DEFAULT_MIN_LEAD_TIME_HOURS (in-code default 0.0). Anything
    else must parse as a finite float in [0.0, MAX_MIN_LEAD_TIME_HOURS]; otherwise
    raise ValueError so the service refuses to start with a typo'd config.
    """
    if env_value is None:
        return DEFAULT_MIN_LEAD_TIME_HOURS
    stripped = env_value.strip()
    if not stripped:
        return DEFAULT_MIN_LEAD_TIME_HOURS
    try:
        value = float(stripped)
    except ValueError as e:
        raise ValueError(
            f"TENNIS_MIN_LEAD_TIME_HOURS must be a number, got {env_value!r}"
        ) from e
    if value != value or value in (float("inf"), float("-inf")):  # NaN / inf
        raise ValueError(
            f"TENNIS_MIN_LEAD_TIME_HOURS must be a finite number, got {env_value!r}"
        )
    if value < 0.0:
        raise ValueError(
            f"TENNIS_MIN_LEAD_TIME_HOURS must be >= 0.0, got {value}"
        )
    if value > MAX_MIN_LEAD_TIME_HOURS:
        raise ValueError(
            f"TENNIS_MIN_LEAD_TIME_HOURS must be <= {MAX_MIN_LEAD_TIME_HOURS} "
            f"(1 week), got {value}"
        )
    return value


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


def _parse_post_window_poll_enabled(env_value: str | None) -> bool:
    """TENNIS_POST_WINDOW_POLL_ENABLED kill switch for the post-window
    cancellation-hunting poll. Default True; only explicit falsy strings
    disable it. Same parse rules as TENNIS_NTP_REQUIRED — an unrecognised
    value (typo) stays True so the feature is not silently dropped.
    """
    if env_value is None:
        return True
    normalized = env_value.strip().lower()
    return normalized not in ("0", "false", "no", "off", "")


def _parse_cancel_duplicates_enabled(env_value: str | None) -> bool:
    """TENNIS_CANCEL_DUPLICATES_ENABLED feature flag for auto-cancel of
    duplicate bookings on multi-success fan-out. Default True; only explicit
    falsy strings disable it. Same fail-safe parse rules as the other
    boolean env helpers — typos stay True.
    """
    if env_value is None:
        return True
    normalized = env_value.strip().lower()
    return normalized not in ("0", "false", "no", "off", "")


def _parse_telegram_enabled(env_value: str | None) -> bool:
    """TELEGRAM_NOTIFICATIONS_ENABLED — opt-in feature flag.

    Inverse of the other boolean helpers: defaults to FALSE so a fresh deploy
    does not start spamming chats until the operator explicitly turns it on
    (and verifies bot_token + chat_ids are populated). Only explicit truthy
    strings enable it; everything else (including unrecognised typos) stays
    disabled — typos must NOT silently start sending notifications.
    """
    if env_value is None:
        return False
    normalized = env_value.strip().lower()
    return normalized in ("1", "true", "yes", "on")


def _build_telegram_notifier(
    env: dict[str, str], logger: logging.Logger
) -> TelegramNotifier:
    """Construct the production TelegramNotifier from env. Logs a one-line
    init summary at startup so the operator can see in journalctl whether
    notifications are live, and why they are off when they are."""
    enabled = _parse_telegram_enabled(env.get("TELEGRAM_NOTIFICATIONS_ENABLED"))
    bot_token_raw = env.get("TELEGRAM_BOT_TOKEN", "").strip()
    bot_token = bot_token_raw or None
    personal = env.get("TELEGRAM_PERSONAL_CHAT_ID", "").strip()
    group = env.get("TELEGRAM_GROUP_CHAT_ID", "").strip()
    chat_ids: tuple[str, ...] = tuple(c for c in (personal, group) if c)

    if not enabled:
        logger.info("telegram_notifier_disabled reason=flag_off")
        return disabled_notifier()
    if bot_token is None:
        logger.warning("telegram_notifier_disabled reason=missing_bot_token")
        return disabled_notifier()
    if not chat_ids:
        logger.warning("telegram_notifier_disabled reason=no_chat_ids")
        return disabled_notifier()

    logger.info(
        "telegram_notifier_initialized enabled=true chat_ids_count=%d",
        len(chat_ids),
    )
    return TelegramNotifier(bot_token=bot_token, chat_ids=chat_ids, enabled=True)


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

    post_window_poll_enabled = _parse_post_window_poll_enabled(
        os.environ.get("TENNIS_POST_WINDOW_POLL_ENABLED")
    )
    if not post_window_poll_enabled:
        logger.warning(
            "post_window_poll_enabled=False (TENNIS_POST_WINDOW_POLL_ENABLED env override) "
            "— post-window cancellation polling is disabled"
        )

    cancel_duplicates_enabled = _parse_cancel_duplicates_enabled(
        os.environ.get("TENNIS_CANCEL_DUPLICATES_ENABLED")
    )
    if not cancel_duplicates_enabled:
        logger.warning(
            "cancel_duplicates_enabled=False (TENNIS_CANCEL_DUPLICATES_ENABLED env override) "
            "— duplicate bookings will only be logged, not cancelled"
        )

    try:
        min_lead_time_hours = _parse_min_lead_time_hours(
            os.environ.get("TENNIS_MIN_LEAD_TIME_HOURS")
        )
    except ValueError as e:
        logger.error("min_lead_time_hours_invalid: %s", e)
        print(f"ERROR: {e}", file=sys.stderr)
        return EXIT_ERROR
    logger.info(
        "min_lead_time_hours=%s (0.0 = guard disabled; per-booking override may apply)",
        min_lead_time_hours,
    )

    store_path = _resolve_store_path()
    try:
        store = FileBookingStore(store_path)
    except ValueError as e:
        logger.error("store_init_failed: %s", e)
        print(
            f"ERROR: cannot initialise FileBookingStore at {store_path}: {e}. "
            "Ensure parent directory exists (mounted /app/data inside container, "
            "/var/lib/tennis-booking/data on host).",
            file=sys.stderr,
        )
        return EXIT_ERROR
    logger.info("store_initialised: path=%s", store_path)

    notifier = _build_telegram_notifier(dict(os.environ), logger)

    async with AltegioClient(altegio_config) as client:
        scheduler_loop = SchedulerLoop(
            app_config,
            client,
            SystemClock(),
            ntp_required=ntp_required,
            store=store,
            min_lead_time_hours=min_lead_time_hours,
            post_window_poll_enabled=post_window_poll_enabled,
            cancel_duplicates_enabled=cancel_duplicates_enabled,
            notifier=notifier,
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

    # CLI subcommand path: short-circuit before logging / config / Altegio
    # init — these subcommands are admin tools and should not require a full
    # service config to be present on disk.
    if args.subcommand == "import-record":
        return await run_import_record(args, _resolve_store_path())

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
