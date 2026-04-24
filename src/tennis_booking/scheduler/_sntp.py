import asyncio
import socket
import struct
import time
from datetime import UTC, datetime

from .clock_errors import NTPResponseError, NTPUnreachableError

# NTP epoch (1900-01-01) is 2208988800 seconds before Unix epoch (1970-01-01).
_NTP_UNIX_EPOCH_DELTA = 2_208_988_800
_NTP_PORT = 123
_NTP_PACKET_SIZE = 48

# LI=0 (0b00) | VN=4 (0b100) | Mode=3 (0b011) → 0x23.
_CLIENT_REQUEST_HEADER = 0x23


def _build_request() -> bytes:
    packet = bytearray(_NTP_PACKET_SIZE)
    packet[0] = _CLIENT_REQUEST_HEADER
    return bytes(packet)


def _parse_response(data: bytes) -> datetime:
    if len(data) != _NTP_PACKET_SIZE:
        raise NTPResponseError("<parser>", f"expected {_NTP_PACKET_SIZE} bytes, got {len(data)}")

    header = data[0]
    leap = (header >> 6) & 0b11
    version = (header >> 3) & 0b111
    stratum = data[1]

    if leap == 3:
        raise NTPResponseError("<parser>", "alarm condition (LI=3)")
    if version not in (3, 4):
        raise NTPResponseError("<parser>", f"unsupported version {version}")
    # stratum 0 = kiss-of-death, stratum 16+ reserved/invalid for usable time.
    if stratum == 0 or stratum > 15:
        raise NTPResponseError("<parser>", f"invalid stratum {stratum}")

    # Transmit Timestamp: bytes 40..47. Seconds (32b) + fraction (32b), big-endian, NTP epoch.
    seconds, fraction = struct.unpack("!II", data[40:48])
    if seconds == 0 and fraction == 0:
        raise NTPResponseError("<parser>", "transmit timestamp is zero")

    unix_seconds = seconds - _NTP_UNIX_EPOCH_DELTA
    unix_fraction = fraction / 2**32
    return datetime.fromtimestamp(unix_seconds + unix_fraction, tz=UTC)


class _SntpProtocol(asyncio.DatagramProtocol):
    def __init__(self, waiter: asyncio.Future[bytes]) -> None:
        self._waiter = waiter

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if not self._waiter.done():
            self._waiter.set_result(data)

    def error_received(self, exc: Exception) -> None:
        if not self._waiter.done():
            self._waiter.set_exception(exc)

    def connection_lost(self, exc: Exception | None) -> None:
        if exc is not None and not self._waiter.done():
            self._waiter.set_exception(exc)


class SntpClient:
    """SNTPv4 client over raw UDP. Stdlib only, non-blocking via asyncio."""

    async def fetch(self, server: str, timeout_s: float) -> tuple[datetime, float]:
        loop = asyncio.get_running_loop()

        try:
            addr_infos = await loop.getaddrinfo(
                server, _NTP_PORT, type=socket.SOCK_DGRAM
            )
        except OSError as exc:
            raise NTPUnreachableError(server, f"DNS resolution failed: {exc}") from exc

        if not addr_infos:
            raise NTPUnreachableError(server, "no address records")

        family, _socktype, _proto, _canon, sockaddr = addr_infos[0]

        waiter: asyncio.Future[bytes] = loop.create_future()

        try:
            transport, _protocol = await loop.create_datagram_endpoint(
                lambda: _SntpProtocol(waiter),
                remote_addr=(sockaddr[0], _NTP_PORT),
                family=family,
            )
        except OSError as exc:
            raise NTPUnreachableError(server, f"socket setup failed: {exc}") from exc

        try:
            request = _build_request()
            t_send = time.perf_counter()
            try:
                transport.sendto(request)
            except OSError as exc:
                raise NTPUnreachableError(server, f"send failed: {exc}") from exc

            try:
                data = await asyncio.wait_for(waiter, timeout=timeout_s)
            except TimeoutError as exc:
                raise NTPUnreachableError(server, f"timeout after {timeout_s}s") from exc
            except OSError as exc:
                raise NTPUnreachableError(server, f"recv failed: {exc}") from exc

            t_recv = time.perf_counter()
            rtt_ms = (t_recv - t_send) * 1000.0

            try:
                ntp_time = _parse_response(data)
            except NTPResponseError as exc:
                raise NTPResponseError(server, str(exc).split(": ", 1)[-1]) from exc

            return ntp_time, rtt_ms
        finally:
            transport.close()
