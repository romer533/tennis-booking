class ClockDriftError(Exception):
    def __init__(self, drift_ms: float, threshold_ms: int, server: str) -> None:
        super().__init__(
            f"Clock drift {drift_ms:.1f}ms exceeds threshold {threshold_ms}ms (vs {server})"
        )
        self.drift_ms = drift_ms
        self.threshold_ms = threshold_ms
        self.server = server


class NTPUnreachableError(Exception):
    def __init__(self, server: str, cause: str) -> None:
        super().__init__(f"NTP server {server!r} unreachable: {cause}")
        self.server = server


class NTPResponseError(NTPUnreachableError):
    """Raised on malformed / kiss-o'-death NTP response."""
