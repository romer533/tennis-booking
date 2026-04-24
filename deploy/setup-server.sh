#!/usr/bin/env bash
# One-time (idempotent) server setup for tennis-booking.
#
# Run as root (sudo) on the target host. Safe to re-run; every step is guarded.
# Does NOT start the service — operator must first populate /etc/tennis-booking/env
# and config YAMLs, then `systemctl start tennis-booking` manually.

set -euo pipefail

SERVICE_USER="whimpy"
SERVICE_GROUP="whimpy"
APP_DIR="/opt/tennis-booking"
CONFIG_DIR="/etc/tennis-booking"
LOG_DIR="/var/log/tennis-booking"
REPO_URL="${REPO_URL:-https://github.com/RomanGoltsov/tennis-booking.git}"
BRANCH="${BRANCH:-main}"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"

log() { printf '[setup] %s\n' "$*" >&2; }
die() { printf '[setup] ERROR: %s\n' "$*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "must run as root (sudo)"

# 1. Prerequisites
command -v "$PYTHON_BIN" >/dev/null 2>&1 || die "$PYTHON_BIN not found; install Python 3.11"
command -v git >/dev/null 2>&1 || die "git not found"
command -v systemctl >/dev/null 2>&1 || die "systemctl not found (not a systemd host?)"

log "Python: $($PYTHON_BIN --version)"

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

# 3. Directories
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0755 "$APP_DIR"
install -d -o root           -g "$SERVICE_GROUP" -m 0750 "$CONFIG_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$LOG_DIR"
log "dirs ok: $APP_DIR, $CONFIG_DIR, $LOG_DIR"

# 4. Clone or update repo
if [[ ! -d "$APP_DIR/.git" ]]; then
    log "cloning $REPO_URL → $APP_DIR"
    sudo -u "$SERVICE_USER" git clone --branch "$BRANCH" "$REPO_URL" "$APP_DIR"
else
    log "updating existing clone at $APP_DIR"
    sudo -u "$SERVICE_USER" git -C "$APP_DIR" fetch origin "$BRANCH"
    sudo -u "$SERVICE_USER" git -C "$APP_DIR" reset --hard "origin/$BRANCH"
fi

# 5. Virtualenv + install
if [[ ! -x "$APP_DIR/venv/bin/python" ]]; then
    log "creating venv"
    sudo -u "$SERVICE_USER" "$PYTHON_BIN" -m venv "$APP_DIR/venv"
fi
log "installing tennis-booking into venv"
sudo -u "$SERVICE_USER" "$APP_DIR/venv/bin/pip" install --upgrade pip --quiet
sudo -u "$SERVICE_USER" "$APP_DIR/venv/bin/pip" install -e "$APP_DIR" --quiet

# 6. Env file stub — create if missing, never overwrite (contains secrets).
if [[ ! -f "$CONFIG_DIR/env" ]]; then
    cat > "$CONFIG_DIR/env" <<'EOF'
# EnvironmentFile for systemd. Edit with real values. DO NOT commit.
ALTEGIO_BEARER_TOKEN=
# ALTEGIO_BASE_URL=https://b551098.alteg.io
# ALTEGIO_COMPANY_ID=521176
# ALTEGIO_BOOKFORM_ID=551098
# ALTEGIO_DRY_RUN=0
# TENNIS_LOG_DIR=/var/log/tennis-booking
EOF
    chown "root:$SERVICE_GROUP" "$CONFIG_DIR/env"
    chmod 0640 "$CONFIG_DIR/env"
    log "created $CONFIG_DIR/env stub — fill ALTEGIO_BEARER_TOKEN before starting"
else
    log "$CONFIG_DIR/env exists — left untouched"
fi

# 7. YAML config stubs
for f in schedule.yaml profiles.yaml; do
    example="$APP_DIR/config/${f%.yaml}.example.yaml"
    target="$CONFIG_DIR/$f"
    if [[ ! -f "$target" && -f "$example" ]]; then
        install -o root -g "$SERVICE_GROUP" -m 0640 "$example" "$target"
        log "copied example → $target (edit before starting)"
    fi
done

# 8. Install systemd unit
install -o root -g root -m 0644 "$APP_DIR/deploy/tennis-booking.service" \
    /etc/systemd/system/tennis-booking.service
systemctl daemon-reload
systemctl enable tennis-booking >/dev/null
log "systemd unit enabled (NOT started — verify config first)"

cat <<EOF

[setup] Done.

Next steps (manual):
  1. Edit $CONFIG_DIR/env  — set ALTEGIO_BEARER_TOKEN
  2. Edit $CONFIG_DIR/schedule.yaml — populate bookings
  3. Edit $CONFIG_DIR/profiles.yaml — populate profiles
  4. Verify NTP:     chronyc tracking   (offset should be < 50ms)
  5. Start service:  sudo systemctl start tennis-booking
  6. Watch logs:     sudo journalctl -u tennis-booking -f
                     tail -f $LOG_DIR/service.log

EOF
