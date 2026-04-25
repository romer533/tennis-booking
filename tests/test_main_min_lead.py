"""Tests for `_parse_min_lead_time_hours` env var parsing in __main__.

Production sets `TENNIS_MIN_LEAD_TIME_HOURS=2` via systemd EnvironmentFile.
In-code default is 0.0 (guard disabled) so tests / dev runs are not affected.
Invalid values fail-fast at startup — silent fallback would mask config typos.
"""
from __future__ import annotations

import pytest

from tennis_booking import __main__ as cli


def test_min_lead_unset_returns_default() -> None:
    assert cli._parse_min_lead_time_hours(None) == cli.DEFAULT_MIN_LEAD_TIME_HOURS
    assert cli._parse_min_lead_time_hours(None) == 0.0


def test_min_lead_explicit_two_hours() -> None:
    assert cli._parse_min_lead_time_hours("2") == 2.0
    assert cli._parse_min_lead_time_hours("2.0") == 2.0


def test_min_lead_explicit_half_hour() -> None:
    assert cli._parse_min_lead_time_hours("0.5") == 0.5


def test_min_lead_zero_disables_guard() -> None:
    assert cli._parse_min_lead_time_hours("0") == 0.0
    assert cli._parse_min_lead_time_hours("0.0") == 0.0


def test_min_lead_empty_string_falls_back_to_default() -> None:
    assert cli._parse_min_lead_time_hours("") == cli.DEFAULT_MIN_LEAD_TIME_HOURS


def test_min_lead_whitespace_falls_back_to_default() -> None:
    assert cli._parse_min_lead_time_hours("   ") == cli.DEFAULT_MIN_LEAD_TIME_HOURS


def test_min_lead_invalid_string_raises() -> None:
    with pytest.raises(ValueError, match="must be a number"):
        cli._parse_min_lead_time_hours("invalid")


def test_min_lead_negative_rejected() -> None:
    with pytest.raises(ValueError, match=">= 0.0"):
        cli._parse_min_lead_time_hours("-1.0")


def test_min_lead_above_max_rejected() -> None:
    with pytest.raises(ValueError, match="<= 168"):
        cli._parse_min_lead_time_hours("200")


def test_min_lead_nan_rejected() -> None:
    with pytest.raises(ValueError, match="finite"):
        cli._parse_min_lead_time_hours("nan")


def test_min_lead_inf_rejected() -> None:
    with pytest.raises(ValueError, match="finite"):
        cli._parse_min_lead_time_hours("inf")


def test_min_lead_boundary_max_accepted() -> None:
    assert cli._parse_min_lead_time_hours("168") == 168.0


def test_min_lead_above_max_with_decimal_rejected() -> None:
    with pytest.raises(ValueError, match="<= 168"):
        cli._parse_min_lead_time_hours("168.01")
