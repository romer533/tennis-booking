import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from ._sntp import SntpClient
from .clock_errors import ClockDriftError, NTPUnreachableError

DEFAULT_SERVER = "time.cloudflare.com"
DEFAULT_THRESHOLD_MS = 50
DEFAULT_TIMEOUT_S = 2.0
DEFAULT_RETRIES = 1
DEFAULT_BACKOFF_S = 0.2


@dataclass(frozen=True, slots=True)
class CheckResult:
    server: str
    ntp_time: datetime
    drift_ms: float
    rtt_ms: float


class NTPClient(Protocol):
    async def fetch(self, server: str, timeout_s: float) -> tuple[datetime, float]: ...


def _validate(*, server: str, threshold_ms: int, timeout_s: float, retries: int) -> None:
    if threshold_ms < 0:
        raise ValueError(f"threshold_ms must be >= 0, got {threshold_ms}")
    if timeout_s <= 0:
        raise ValueError(f"timeout_s must be > 0, got {timeout_s}")
    if retries < 0:
        raise ValueError(f"retries must be >= 0, got {retries}")
    if not server.strip():
        raise ValueError("server must not be empty")


async def check_ntp_drift(
    *,
    server: str = DEFAULT_SERVER,
    threshold_ms: int = DEFAULT_THRESHOLD_MS,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    retries: int = DEFAULT_RETRIES,
    backoff_s: float = DEFAULT_BACKOFF_S,
    client: NTPClient | None = None,
) -> CheckResult:
    _validate(server=server, threshold_ms=threshold_ms, timeout_s=timeout_s, retries=retries)

    ntp_client: NTPClient = client if client is not None else SntpClient()

    total_attempts = retries + 1
    last_exc: NTPUnreachableError | None = None

    for attempt in range(total_attempts):
        try:
            ntp_time, rtt_ms = await ntp_client.fetch(server, timeout_s)
        except NTPUnreachableError as exc:
            last_exc = exc
            if attempt < total_attempts - 1:
                await asyncio.sleep(backoff_s)
            continue

        local_now = datetime.now(tz=UTC)
        drift_ms = (local_now - ntp_time).total_seconds() * 1000.0

        if abs(drift_ms) > threshold_ms:
            raise ClockDriftError(drift_ms, threshold_ms, server)

        return CheckResult(server=server, ntp_time=ntp_time, drift_ms=drift_ms, rtt_ms=rtt_ms)

    assert last_exc is not None  # loop exited only via except branch
    # Preserve concrete error type (NTPResponseError is a subclass) so callers
    # can distinguish transport failure from protocol failure after retries.
    raise last_exc
