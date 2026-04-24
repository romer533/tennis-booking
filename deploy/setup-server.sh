#!/usr/bin/env bash
# One-time (idempotent) server setup for tennis-booking, Docker edition.
#
# Run as root (sudo) on the target host. Safe to re-run; every step is guarded.
# Does NOT start the service — operator must first populate /etc/tennis-booking/env
# and the YAML configs in /var/lib/tennis-booking/config, then
# `systemctl start tennis-booking` manually.
#
# This script may also git-clone the repo to /opt/tennis-booking purely as a
# convenience for the operator (so they have unit/sudoers files at hand for
# install/upgrade). The runtime itself runs from the OCI image pulled from
# ghcr.io and does NOT use anything in /opt/tennis-booking.

set -euo pipefail

SERVICE_USER="whimpy"
SERVICE_GROUP="whimpy"
SERVICE_UID="1000"
SERVICE_GID="1000"
APP_DIR="/opt/tennis-booking"            # repo checkout (convenience only)
CONFIG_DIR="/etc/tennis-booking"         # env file (root:whimpy 0640)
STATE_DIR="/var/lib/tennis-booking"      # config/, logs/, data/ — bind-mounted into the container
REPO_URL="${REPO_URL:-https://github.com/RomanGoltsov/tennis-booking.git}"
BRANCH="${BRANCH:-main}"

log() { printf '[setup] %s\n' "$*" >&2; }
die() { printf '[setup] ERROR: %s\n' "$*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "must run as root (sudo)"

# 1. Prerequisites
command -v docker >/dev/null 2>&1 || die \
    "docker not found. Install: https://docs.docker.com/engine/install/  (then re-run)"
command -v systemctl >/dev/null 2>&1 || die "systemctl not found (not a systemd host?)"
command -v git >/dev/null 2>&1 || log "WARNING: git missing — will skip /opt/tennis-booking checkout"

log "docker: $(docker --version)"

# NTP check — non-fatal warning (host may use systemd-timesyncd instead).
if command -v chronyc >/dev/null 2>&1; then
    if ! chronyc tracking >/dev/null 2>&1; then
        log "WARNING: chronyc tracking failed — NTP may not be synced. Fix before go-live."
    else
        log "NTP (chrony) reachable."
    fi
elif command -v timedatectl >/dev/null 2>&1; then
    if timedatectl show --property=NTPSynchronized --value | grep -qx yes; then
        log "NTP (timedatectl) synced."
    else
        log "WARNING: NTPSynchronized=no — enable chrony or systemd-timesyncd before go-live."
    fi
else
    log "WARNING: no chrony / timedatectl; cannot verify NTP."
fi

# 2. Service user (do not create — `whimpy` is the login user on this host).
id "$SERVICE_USER" >/dev/null 2>&1 || die "user $SERVICE_USER does not exist"

# Sanity: container runs as uid 1000 — host bind-mounted dirs must be 1000:1000.
host_uid=$(id -u "$SERVICE_USER")
host_gid=$(id -g "$SERVICE_USER")
if [[ "$host_uid" != "$SERVICE_UID" || "$host_gid" != "$SERVICE_GID" ]]; then
    log "WARNING: $SERVICE_USER is uid:gid $host_uid:$host_gid — container runs as $SERVICE_UID:$SERVICE_GID."
    log "         logs/data writes from the container will land owned by $SERVICE_UID:$SERVICE_GID,"
    log "         which $SERVICE_USER may not be able to read directly. Fix uid mapping or accept it."
fi

# 3. Directories
#    /etc/tennis-booking/env           — secrets (root:whimpy 0640)
#    /var/lib/tennis-booking/config    — YAML configs (root:whimpy 0750, ro mount)
#    /var/lib/tennis-booking/logs      — RotatingFileHandler sink (1000:1000 0750)
#    /var/lib/tennis-booking/data      — future SQLite (1000:1000 0750)
install -d -o root              -g "$SERVICE_GROUP" -m 0750 "$CONFIG_DIR"
install -d -o root              -g "$SERVICE_GROUP" -m 0750 "$STATE_DIR"
install -d -o root              -g "$SERVICE_GROUP" -m 0750 "$STATE_DIR/config"
install -d -o "$SERVICE_UID"    -g "$SERVICE_GID"   -m 0750 "$STATE_DIR/logs"
install -d -o "$SERVICE_UID"    -g "$SERVICE_GID"   -m 0750 "$STATE_DIR/data"
log "dirs ok: $CONFIG_DIR, $STATE_DIR/{config,logs,data}"

# 4. Optional repo checkout (operator convenience — NOT used at runtime)
if command -v git >/dev/null 2>&1; then
    install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0755 "$APP_DIR"
    if [[ ! -d "$APP_DIR/.git" ]]; then
        log "cloning $REPO_URL → $APP_DIR (for unit/sudoers convenience)"
        sudo -u "$SERVICE_USER" git clone --branch "$BRANCH" "$REPO_URL" "$APP_DIR"
    else
        log "updating existing clone at $APP_DIR"
        sudo -u "$SERVICE_USER" git -C "$APP_DIR" fetch origin "$BRANCH"
        sudo -u "$SERVICE_USER" git -C "$APP_DIR" reset --hard "origin/$BRANCH"
    fi
fi

# 5. Env file stub — create if missing, never overwrite (contains secrets).
if [[ ! -f "$CONFIG_DIR/env" ]]; then
    cat > "$CONFIG_DIR/env" <<'EOF'
# EnvironmentFile for the tennis-booking container. Edit with real values.
# DO NOT commit. Loaded via `docker run --env-file`.
ALTEGIO_BEARER_TOKEN=
# ALTEGIO_BASE_URL=https://b551098.alteg.io
# ALTEGIO_COMPANY_ID=521176
# ALTEGIO_BOOKFORM_ID=551098
# ALTEGIO_DRY_RUN=0
# TENNIS_LOG_DIR=/app/logs        # default; overrides only for unusual setups
# TENNIS_NTP_REQUIRED=1            # set 0 only in dev with no NTP
EOF
    chown "root:$SERVICE_GROUP" "$CONFIG_DIR/env"
    chmod 0640 "$CONFIG_DIR/env"
    log "created $CONFIG_DIR/env stub — fill ALTEGIO_BEARER_TOKEN before starting"
else
    log "$CONFIG_DIR/env exists — left untouched"
fi

# 6. YAML config stubs (read-only mount target)
if [[ -d "$APP_DIR/config" ]]; then
    for f in schedule.yaml profiles.yaml; do
        example="$APP_DIR/config/${f%.yaml}.example.yaml"
        target="$STATE_DIR/config/$f"
        if [[ ! -f "$target" && -f "$example" ]]; then
            install -o root -g "$SERVICE_GROUP" -m 0640 "$example" "$target"
            log "copied example → $target (edit before starting)"
        fi
    done
else
    log "no $APP_DIR/config — skipping example copy. Place schedule.yaml/profiles.yaml in $STATE_DIR/config/ manually."
fi

# 7. Install systemd unit
if [[ -f "$APP_DIR/deploy/tennis-booking.service" ]]; then
    install -o root -g root -m 0644 "$APP_DIR/deploy/tennis-booking.service" \
        /etc/systemd/system/tennis-booking.service
    systemctl daemon-reload
    systemctl enable tennis-booking >/dev/null
    log "systemd unit enabled (NOT started — verify config first)"
else
    log "WARNING: $APP_DIR/deploy/tennis-booking.service missing — install the unit manually."
fi

cat <<EOF

[setup] Done.

Next steps (manual):
  1. Edit $CONFIG_DIR/env  — set ALTEGIO_BEARER_TOKEN
  2. Place configs:
       $STATE_DIR/config/schedule.yaml
       $STATE_DIR/config/profiles.yaml
  3. Install sudoers entry (one-time):
       sudo install -o root -g root -m 0440 \\
           $APP_DIR/deploy/tennis-booking.sudoers \\
           /etc/sudoers.d/tennis-booking
       sudo visudo -cf /etc/sudoers.d/tennis-booking
  4. Verify NTP:     chronyc tracking   (offset should be < 50ms)
  5. Pull image:     sudo docker pull ghcr.io/romer533/tennis-booking:latest
  6. Start service:  sudo systemctl start tennis-booking
  7. Watch logs:     sudo journalctl -u tennis-booking -f
                     tail -f $STATE_DIR/logs/service.log

EOF
