"""Unit tests for env parsing of Telegram-related vars in __main__."""
from __future__ import annotations

import logging

import pytest

from tennis_booking import __main__ as cli


def test_telegram_enabled_default_false() -> None:
    """No env var → disabled (opt-in feature)."""
    assert cli._parse_telegram_enabled(None) is False


def test_telegram_enabled_explicit_true() -> None:
    assert cli._parse_telegram_enabled("1") is True
    assert cli._parse_telegram_enabled("true") is True
    assert cli._parse_telegram_enabled("TRUE") is True
    assert cli._parse_telegram_enabled(" yes ") is True
    assert cli._parse_telegram_enabled("on") is True


def test_telegram_enabled_explicit_false() -> None:
    assert cli._parse_telegram_enabled("0") is False
    assert cli._parse_telegram_enabled("false") is False
    assert cli._parse_telegram_enabled("") is False
    # Unrecognised → fail-safe disabled (opposite of NTP_REQUIRED's fail-safe).
    assert cli._parse_telegram_enabled("maybe") is False


def test_build_telegram_notifier_default_disabled() -> None:
    """Empty env → disabled notifier, no warnings."""
    logger = logging.getLogger("tennis_booking.test")
    notifier = cli._build_telegram_notifier({}, logger)
    assert notifier.is_active is False


def test_build_telegram_notifier_explicit_enabled() -> None:
    env = {
        "TELEGRAM_NOTIFICATIONS_ENABLED": "1",
        "TELEGRAM_BOT_TOKEN": "real-token",
        "TELEGRAM_PERSONAL_CHAT_ID": "111",
        "TELEGRAM_GROUP_CHAT_ID": "-222",
    }
    logger = logging.getLogger("tennis_booking.test")
    notifier = cli._build_telegram_notifier(env, logger)
    assert notifier.is_active is True


def test_build_telegram_notifier_partial_env_no_token_disabled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """flag=true but bot_token missing → disabled, WARN logged."""
    env = {
        "TELEGRAM_NOTIFICATIONS_ENABLED": "1",
        "TELEGRAM_BOT_TOKEN": "",
        "TELEGRAM_PERSONAL_CHAT_ID": "111",
    }
    logger = logging.getLogger("tennis_booking.test_no_token")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        notifier = cli._build_telegram_notifier(env, logger)
    assert notifier.is_active is False
    assert any("missing_bot_token" in rec.getMessage() for rec in caplog.records)


def test_build_telegram_notifier_no_chat_ids_disabled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = {
        "TELEGRAM_NOTIFICATIONS_ENABLED": "1",
        "TELEGRAM_BOT_TOKEN": "tok",
        "TELEGRAM_PERSONAL_CHAT_ID": "",
        "TELEGRAM_GROUP_CHAT_ID": "",
    }
    logger = logging.getLogger("tennis_booking.test_no_chats")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        notifier = cli._build_telegram_notifier(env, logger)
    assert notifier.is_active is False
    assert any("no_chat_ids" in rec.getMessage() for rec in caplog.records)


def test_build_telegram_notifier_only_personal_chat_works() -> None:
    """One of the two chat_ids is sufficient — group is optional."""
    env = {
        "TELEGRAM_NOTIFICATIONS_ENABLED": "1",
        "TELEGRAM_BOT_TOKEN": "tok",
        "TELEGRAM_PERSONAL_CHAT_ID": "111",
    }
    logger = logging.getLogger("tennis_booking.test_one_chat")
    notifier = cli._build_telegram_notifier(env, logger)
    assert notifier.is_active is True


def test_build_telegram_notifier_flag_off_with_full_creds_still_disabled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Even with token + chats, flag=0 → disabled (operator kill-switch)."""
    env = {
        "TELEGRAM_NOTIFICATIONS_ENABLED": "0",
        "TELEGRAM_BOT_TOKEN": "tok",
        "TELEGRAM_PERSONAL_CHAT_ID": "111",
    }
    logger = logging.getLogger("tennis_booking.test_flag_off")
    with caplog.at_level(logging.INFO, logger=logger.name):
        notifier = cli._build_telegram_notifier(env, logger)
    assert notifier.is_active is False
    assert any("flag_off" in rec.getMessage() for rec in caplog.records)
