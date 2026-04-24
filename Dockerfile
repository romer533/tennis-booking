# syntax=docker/dockerfile:1.7
#
# Multi-stage image for tennis-booking. Build once in CI, ship to ghcr.io,
# pull on the server. Mirrors the deployment shape of the registration-telegram-bot
# service that already runs on the same host.

# ---------- Stage 1: builder ----------
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

# Build wheels into an isolated venv. Copy only what pip needs to resolve
# dependencies first so the layer cache survives source-only edits.
RUN python -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --upgrade pip \
 && pip install .

# ---------- Stage 2: runtime ----------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/venv/bin:$PATH" \
    TENNIS_LOG_DIR=/app/logs

# Non-root user matching the host's `whimpy` (uid 1000) — bind-mounted
# /app/logs and /app/data on the host need to be owned by 1000:1000 so the
# container can write to them.
RUN groupadd --system --gid 1000 app \
 && useradd --system --uid 1000 --gid 1000 --home /app --shell /usr/sbin/nologin app

WORKDIR /app

COPY --from=builder /app/venv /app/venv

# Pre-create mount points so a fresh container without host bind mounts still works
# (handy for `docker run --rm ...` smoke tests). Real prod runs mount these from host.
RUN mkdir -p /app/config /app/logs /app/data \
 && chown -R app:app /app

USER app

ENTRYPOINT ["/app/venv/bin/python", "-m", "tennis_booking"]
CMD ["--config-dir=/app/config"]
