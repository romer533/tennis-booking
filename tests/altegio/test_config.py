from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from tennis_booking.altegio import AltegioConfig, AltegioConfigError
from tennis_booking.altegio.config import (
    DEFAULT_BASE_URL,
    DEFAULT_BOOKFORM_ID,
    DEFAULT_COMPANY_ID,
)

VALID_TOKEN = "test-bearer-secret"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "ALTEGIO_BEARER_TOKEN",
        "ALTEGIO_BASE_URL",
        "ALTEGIO_COMPANY_ID",
        "ALTEGIO_BOOKFORM_ID",
        "ALTEGIO_DRY_RUN",
    ):
        monkeypatch.delenv(var, raising=False)


class TestFromEnv:
    def test_missing_token_raises_config_error(self) -> None:
        with pytest.raises(AltegioConfigError, match="ALTEGIO_BEARER_TOKEN"):
            AltegioConfig.from_env()

    def test_empty_token_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", "")
        with pytest.raises(AltegioConfigError, match="ALTEGIO_BEARER_TOKEN"):
            AltegioConfig.from_env()

    def test_whitespace_token_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", "   \t  ")
        with pytest.raises(AltegioConfigError, match="ALTEGIO_BEARER_TOKEN"):
            AltegioConfig.from_env()

    def test_valid_token_uses_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        cfg = AltegioConfig.from_env()
        assert cfg.bearer_token.get_secret_value() == VALID_TOKEN
        assert cfg.base_url == DEFAULT_BASE_URL
        assert cfg.company_id == DEFAULT_COMPANY_ID
        assert cfg.bookform_id == DEFAULT_BOOKFORM_ID
        assert cfg.dry_run is False

    def test_token_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", f"  {VALID_TOKEN}  ")
        cfg = AltegioConfig.from_env()
        assert cfg.bearer_token.get_secret_value() == VALID_TOKEN

    def test_base_url_override_valid(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        monkeypatch.setenv("ALTEGIO_BASE_URL", "https://other.alteg.io")
        cfg = AltegioConfig.from_env()
        assert cfg.base_url == "https://other.alteg.io"

    def test_base_url_override_strips_trailing_slash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        monkeypatch.setenv("ALTEGIO_BASE_URL", "https://other.alteg.io/")
        cfg = AltegioConfig.from_env()
        assert cfg.base_url == "https://other.alteg.io"

    def test_base_url_http_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        monkeypatch.setenv("ALTEGIO_BASE_URL", "http://insecure.alteg.io")
        with pytest.raises(AltegioConfigError, match="https://"):
            AltegioConfig.from_env()

    def test_company_id_int_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        monkeypatch.setenv("ALTEGIO_COMPANY_ID", "999")
        cfg = AltegioConfig.from_env()
        assert cfg.company_id == 999

    def test_company_id_non_int_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        monkeypatch.setenv("ALTEGIO_COMPANY_ID", "abc")
        with pytest.raises(AltegioConfigError, match="ALTEGIO_COMPANY_ID"):
            AltegioConfig.from_env()

    def test_bookform_id_int_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        monkeypatch.setenv("ALTEGIO_BOOKFORM_ID", "12345")
        cfg = AltegioConfig.from_env()
        assert cfg.bookform_id == 12345

    def test_bookform_id_non_int_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        monkeypatch.setenv("ALTEGIO_BOOKFORM_ID", "x12")
        with pytest.raises(AltegioConfigError, match="ALTEGIO_BOOKFORM_ID"):
            AltegioConfig.from_env()

    @pytest.mark.parametrize("value", ["1", "true", "yes", "on", "TRUE", "Yes", " On "])
    def test_dry_run_truthy(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        monkeypatch.setenv("ALTEGIO_DRY_RUN", value)
        cfg = AltegioConfig.from_env()
        assert cfg.dry_run is True

    @pytest.mark.parametrize("value", ["0", "false", "no", "off", "", "maybe", "2"])
    def test_dry_run_falsy(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        monkeypatch.setenv("ALTEGIO_DRY_RUN", value)
        cfg = AltegioConfig.from_env()
        assert cfg.dry_run is False

    def test_dry_run_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", VALID_TOKEN)
        cfg = AltegioConfig.from_env()
        assert cfg.dry_run is False


class TestSecurityProperties:
    def test_repr_does_not_contain_token(self) -> None:
        cfg = AltegioConfig(bearer_token=SecretStr("super-secret-xyz"))
        assert "super-secret-xyz" not in repr(cfg)
        assert "super-secret-xyz" not in str(cfg)

    def test_model_dump_does_not_contain_token(self) -> None:
        cfg = AltegioConfig(bearer_token=SecretStr("super-secret-xyz"))
        dumped = cfg.model_dump()
        assert "super-secret-xyz" not in str(dumped)

    def test_model_dump_json_does_not_contain_token(self) -> None:
        cfg = AltegioConfig(bearer_token=SecretStr("super-secret-xyz"))
        assert "super-secret-xyz" not in cfg.model_dump_json()

    def test_frozen_config(self) -> None:
        cfg = AltegioConfig(bearer_token=SecretStr(VALID_TOKEN))
        with pytest.raises(ValidationError):
            cfg.bearer_token = SecretStr("other")  # type: ignore[misc]


class TestDirectConstruction:
    def test_empty_token_via_constructor_raises(self) -> None:
        with pytest.raises(ValidationError):
            AltegioConfig(bearer_token=SecretStr(""))

    def test_zero_company_id_raises(self) -> None:
        with pytest.raises(ValidationError):
            AltegioConfig(bearer_token=SecretStr(VALID_TOKEN), company_id=0)

    def test_negative_bookform_id_raises(self) -> None:
        with pytest.raises(ValidationError):
            AltegioConfig(bearer_token=SecretStr(VALID_TOKEN), bookform_id=-1)

    def test_invalid_base_url_raises(self) -> None:
        with pytest.raises(ValidationError):
            AltegioConfig(bearer_token=SecretStr(VALID_TOKEN), base_url="ftp://x")

    def test_extra_field_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            AltegioConfig(  # type: ignore[call-arg]
                bearer_token=SecretStr(VALID_TOKEN), extra_field="x"
            )
