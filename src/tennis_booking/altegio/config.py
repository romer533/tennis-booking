from __future__ import annotations

import os

from pydantic import BaseModel, ConfigDict, SecretStr, field_validator

from .errors import AltegioConfigError

_TRUTHY = frozenset({"1", "true", "yes", "on"})

DEFAULT_BASE_URL = "https://b551098.alteg.io"
DEFAULT_COMPANY_ID = 521176
DEFAULT_BOOKFORM_ID = 551098


class AltegioConfig(BaseModel):
    """Конфиг клиента Altegio. Bearer-токен в SecretStr → автоматическая маскировка."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    bearer_token: SecretStr
    base_url: str = DEFAULT_BASE_URL
    company_id: int = DEFAULT_COMPANY_ID
    bookform_id: int = DEFAULT_BOOKFORM_ID
    dry_run: bool = False

    @field_validator("bearer_token")
    @classmethod
    def _validate_bearer(cls, v: SecretStr) -> SecretStr:
        if not v.get_secret_value().strip():
            raise ValueError("bearer_token must not be empty")
        return v

    @field_validator("base_url")
    @classmethod
    def _validate_base_url(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith("https://"):
            raise ValueError(f"base_url must start with 'https://', got {v!r}")
        return v.rstrip("/")

    @field_validator("company_id", "bookform_id")
    @classmethod
    def _positive_int(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"id must be > 0, got {v}")
        return v

    @classmethod
    def from_env(cls) -> AltegioConfig:
        token_raw = os.environ.get("ALTEGIO_BEARER_TOKEN", "")
        if not token_raw.strip():
            raise AltegioConfigError("set ALTEGIO_BEARER_TOKEN env var")

        kwargs: dict[str, object] = {"bearer_token": SecretStr(token_raw.strip())}

        if (base_url := os.environ.get("ALTEGIO_BASE_URL")) is not None:
            base_url = base_url.strip()
            if not base_url.startswith("https://"):
                raise AltegioConfigError(
                    f"ALTEGIO_BASE_URL must start with 'https://', got {base_url!r}"
                )
            kwargs["base_url"] = base_url

        if (company_id := os.environ.get("ALTEGIO_COMPANY_ID")) is not None:
            try:
                kwargs["company_id"] = int(company_id)
            except ValueError as e:
                raise AltegioConfigError(
                    f"ALTEGIO_COMPANY_ID must be int, got {company_id!r}"
                ) from e

        if (bookform_id := os.environ.get("ALTEGIO_BOOKFORM_ID")) is not None:
            try:
                kwargs["bookform_id"] = int(bookform_id)
            except ValueError as e:
                raise AltegioConfigError(
                    f"ALTEGIO_BOOKFORM_ID must be int, got {bookform_id!r}"
                ) from e

        kwargs["dry_run"] = _parse_truthy(os.environ.get("ALTEGIO_DRY_RUN"))

        try:
            return cls(**kwargs)  # type: ignore[arg-type]
        except ValueError as e:
            raise AltegioConfigError(str(e)) from e


def _parse_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY
