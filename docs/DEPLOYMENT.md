# Deployment

Production deployment of `tennis-booking` to `194.195.241.83` (user `whimpy`, SSH port `13022`).

## Architecture

```
┌─────────────────┐   push to main    ┌──────────────────────┐
│   GitHub repo   │ ───────────────▶  │  GitHub Actions      │
└─────────────────┘                   │  (.github/workflows/ │
                                      │   deploy.yml)        │
                                      └──────────┬───────────┘
                                                 │ ssh (key auth)
                                                 ▼
                                      ┌──────────────────────┐
                                      │  whimpy@194.195...   │
                                      │  /opt/tennis-booking │
                                      │  git reset + pip     │
                                      │  + systemctl restart │
                                      └──────────┬───────────┘
                                                 │
                             ┌───────────────────┴──────────────────┐
                             ▼                                      ▼
                  ┌──────────────────────┐            ┌────────────────────────┐
                  │ systemd              │            │ RotatingFileHandler    │
                  │ tennis-booking.svc   │  ───────▶  │ /var/log/tennis-       │
                  │ Type=simple, user=   │            │   booking/service.log  │
                  │ whimpy, ExecStart=   │            │ (max 10MB × 14 files)  │
                  │ python -m tennis_    │            └────────────────────────┘
                  │ booking              │            ┌────────────────────────┐
                  └──────────────────────┘  ───────▶  │ journalctl             │
                                                      │  (stdout/stderr sink)  │
                                                      └────────────────────────┘
```

Key paths on the server:

| Path                              | Purpose                             | Owner         | Mode |
| --------------------------------- | ----------------------------------- | ------------- | ---- |
| `/opt/tennis-booking`             | Git checkout + venv                 | whimpy:whimpy | 0755 |
| `/opt/tennis-booking/venv`        | Python virtualenv                   | whimpy:whimpy | 0755 |
| `/etc/tennis-booking/env`         | EnvironmentFile (bearer token etc.) | root:whimpy   | 0640 |
| `/etc/tennis-booking/schedule.yaml` | Booking schedule                 | root:whimpy   | 0640 |
| `/etc/tennis-booking/profiles.yaml` | Client profiles (PII)            | root:whimpy   | 0640 |
| `/var/log/tennis-booking`         | RotatingFileHandler sink            | whimpy:whimpy | 0750 |
| `/etc/systemd/system/tennis-booking.service` | Unit file                | root:root     | 0644 |
| `/etc/sudoers.d/tennis-booking`   | NOPASSWD entry for systemctl        | root:root     | 0440 |

## Prerequisites

Server (Ubuntu / Debian assumed):

- Python 3.11+ (`apt install python3.11 python3.11-venv`)
- Git, systemd, OpenSSH server
- Time sync: `chrony` (preferred) or `systemd-timesyncd`
- Outbound firewall:
  - TCP 443 → `b551098.alteg.io`
  - UDP 123 → `pool.ntp.org` (for in-app NTP drift check)

Workstation:

- `ssh-keygen`, `ssh`, `ssh-copy-id`
- Access to the GitHub repository **Settings → Secrets and variables → Actions**

## One-time setup

### 1. Generate a deploy SSH keypair (on your workstation)

Dedicated key — do **not** reuse your personal SSH key.

```bash
ssh-keygen -t ed25519 -f ~/.ssh/tennis-booking-deploy -N "" \
    -C "github-actions-deploy@tennis-booking"
```

This creates:

- `~/.ssh/tennis-booking-deploy` (private — goes into GitHub secret)
- `~/.ssh/tennis-booking-deploy.pub` (public — goes on the server)

### 2. Install the public key on the server

```bash
ssh-copy-id -i ~/.ssh/tennis-booking-deploy.pub -p 13022 whimpy@194.195.241.83
```

Or manually append `~/.ssh/tennis-booking-deploy.pub` to `/home/whimpy/.ssh/authorized_keys`.

### 3. Smoke-test the key

```bash
ssh -i ~/.ssh/tennis-booking-deploy -p 13022 whimpy@194.195.241.83 hostname
```

Must print the server hostname without prompting for a password.

### 4. Capture the host fingerprint

```bash
ssh-keyscan -p 13022 194.195.241.83
```

Copy the output — it becomes the `DEPLOY_HOST_KEY` secret. Using `StrictHostKeyChecking=yes` (the default) against a pinned key is how we protect against MITM at deploy time.

### 5. Create GitHub Secrets

Repository → **Settings → Secrets and variables → Actions → New repository secret**.

| Name              | Value                                                      |
| ----------------- | ---------------------------------------------------------- |
| `DEPLOY_SSH_KEY`  | Contents of `~/.ssh/tennis-booking-deploy` (private key, full file including header/footer) |
| `DEPLOY_HOST_KEY` | Output of `ssh-keyscan -p 13022 194.195.241.83` (one or more `194.195.241.83 ssh-ed25519 ...` lines) |
| `DEPLOY_HOST`     | `194.195.241.83`                                           |
| `DEPLOY_PORT`     | `13022`                                                    |
| `DEPLOY_USER`     | `whimpy`                                                   |

### 6. Bootstrap the server

SSH to the server and run the setup script:

```bash
ssh -p 13022 whimpy@194.195.241.83

# once on the server:
sudo apt update
sudo apt install -y python3.11 python3.11-venv git chrony

# clone temporarily to get the setup script (setup-server.sh re-clones to /opt)
git clone https://github.com/RomanGoltsov/tennis-booking.git /tmp/tennis-booking
sudo bash /tmp/tennis-booking/deploy/setup-server.sh
rm -rf /tmp/tennis-booking
```

The script is idempotent — safe to re-run (e.g. to upgrade Python or recreate the venv).

### 7. Populate secrets and config on the server

```bash
# 7a. Bearer token + any overrides
sudo -e /etc/tennis-booking/env
# Edit ALTEGIO_BEARER_TOKEN=...  (value captured from Phase 0 DevTools research)
# Leave ALTEGIO_DRY_RUN commented (defaults to false = real bookings).

# 7b. Schedule + profiles
sudo -e /etc/tennis-booking/schedule.yaml
sudo -e /etc/tennis-booking/profiles.yaml
# Fill real court_id / service_id / names / phones.
```

File modes should already be `0640 root:whimpy` from setup-server.sh. Confirm:

```bash
ls -l /etc/tennis-booking/
# -rw-r----- root whimpy env
# -rw-r----- root whimpy profiles.yaml
# -rw-r----- root whimpy schedule.yaml
```

### 8. Install the sudoers entry

The GitHub Actions workflow invokes `sudo systemctl restart tennis-booking` on the server; `whimpy` needs NOPASSWD privilege for exactly those commands.

```bash
sudo install -o root -g root -m 0440 \
    /opt/tennis-booking/deploy/tennis-booking.sudoers \
    /etc/sudoers.d/tennis-booking

# Validate — never edit sudoers without visudo's syntax check
sudo visudo -cf /etc/sudoers.d/tennis-booking
# → "parsed OK"
```

If `visudo -c` fails, `rm /etc/sudoers.d/tennis-booking` immediately; a broken sudoers file can lock you out.

### 9. Verify NTP

Offset must be < 50 ms — the scheduler's `clock.py` NTP check enforces this at startup (`ntp_threshold_ms=50`).

```bash
chronyc tracking | grep -E "Leap|Last offset|System time"
# Leap status     : Normal
# System time     : 0.00001234 seconds slow of NTP time  ← |offset| < 0.050
```

If drift is high: `sudo systemctl restart chrony && sleep 30 && chronyc tracking`.

### 10. First manual start

```bash
sudo systemctl start tennis-booking
sudo systemctl status tennis-booking --no-pager
```

Expected: `Active: active (running)`. If it's `activating (auto-restart)` — check logs:

```bash
sudo journalctl -u tennis-booking -n 50 --no-pager
```

### 11. Live monitoring

Two equivalent views; keep both handy during the first week:

```bash
# systemd journal — captures stdout/stderr incl. any crash trace
sudo journalctl -u tennis-booking -f

# structlog JSON file — survives systemd cycling, grep-friendly
sudo tail -f /var/log/tennis-booking/service.log
```

## Continuous deployment

After the one-time setup, every `git push origin main` triggers `.github/workflows/deploy.yml`:

1. GitHub Actions SSHs to `whimpy@194.195.241.83:13022`.
2. On the server: `git fetch && git reset --hard origin/main && pip install -e . && systemctl restart`.
3. Post-restart: polls `systemctl is-active` up to 5s; dumps last 20 journal lines.

Concurrency is `deploy-production` with `cancel-in-progress: false` — a second push queues behind the first rather than replacing it.

Manual trigger: repository → **Actions → deploy → Run workflow**.

## Rollback

Rollback is manual (no automated workflow — a bad deploy is rare and diagnosing it beats re-running a fragile workflow). To roll back to the previous commit:

```bash
ssh -p 13022 whimpy@194.195.241.83

# On server:
cd /opt/tennis-booking
git log --oneline -5        # identify last-known-good SHA
git reset --hard <SHA>
./venv/bin/pip install -e . --quiet
sudo systemctl restart tennis-booking
sudo systemctl is-active tennis-booking
sudo journalctl -u tennis-booking -n 30 --no-pager
```

If the bad commit is already merged to `main`, follow up with `git revert` locally and push — otherwise the next deploy re-applies the broken code.

## Troubleshooting

### `ClockDriftError: drift 123ms > threshold 50ms`

NTP is lagging. `chronyc tracking` → check `Last offset`, `System time`. Fix:

```bash
sudo systemctl restart chrony
sleep 30
chronyc sources -v
chronyc tracking
```

Temporary workaround: set `TENNIS_NTP_REQUIRED=0` in `/etc/tennis-booking/env` (not yet wired; for now drift errors crash startup — fix NTP properly).

### `AltegioConfigError: set ALTEGIO_BEARER_TOKEN env var`

`/etc/tennis-booking/env` is missing or doesn't contain `ALTEGIO_BEARER_TOKEN=...`. Verify:

```bash
sudo cat /etc/tennis-booking/env
# Must contain a non-empty line: ALTEGIO_BEARER_TOKEN=eyJ...

sudo systemctl cat tennis-booking | grep EnvironmentFile
# EnvironmentFile=/etc/tennis-booking/env
```

### `ConfigError: schedule.yaml not found`

```bash
sudo ls -l /etc/tennis-booking/
# If missing: copy from example
sudo cp /opt/tennis-booking/config/schedule.example.yaml /etc/tennis-booking/schedule.yaml
sudo chown root:whimpy /etc/tennis-booking/schedule.yaml
sudo chmod 0640 /etc/tennis-booking/schedule.yaml
sudo -e /etc/tennis-booking/schedule.yaml
```

### `sudo: a password is required` (in GitHub Actions log)

Sudoers entry missing or not parsed. Reinstall (see step 8) and validate with `sudo visudo -cf /etc/sudoers.d/tennis-booking`.

### `Permission denied (publickey)` (in GitHub Actions log)

- `DEPLOY_SSH_KEY` secret was pasted with wrong line endings. Re-copy the full file including `-----BEGIN OPENSSH PRIVATE KEY-----` header/footer.
- Public key not installed on server: `ssh whimpy@... 'cat .ssh/authorized_keys' | grep github-actions-deploy`.

### Log file not rotating

Check handler config lands at runtime:

```bash
sudo ls -lh /var/log/tennis-booking/
# Expect: service.log and, after 10 MB, service.log.1, service.log.2, ...
# backupCount=14, so max 15 files total (current + 14 old).
```

If only `service.log` grows past 10 MB, the handler didn't install — likely `setup_logging` threw on `log_dir.mkdir` (permission). Verify `/var/log/tennis-booking` is owned by `whimpy`.

### Service dies immediately on start

```bash
sudo systemctl status tennis-booking --no-pager
sudo journalctl -u tennis-booking -n 100 --no-pager
```

Most common root causes listed above. If none match, paste the last 100 lines into an issue.

### How to stop the service temporarily

```bash
sudo systemctl stop tennis-booking         # stop, keep enabled (starts on boot)
sudo systemctl disable --now tennis-booking  # stop, don't start on boot
```

`TimeoutStopSec=70` in the unit file means `systemctl stop` waits up to 70s for in-flight booking attempts to finish — by design. Do **not** `kill -9` the service between `T-30s` and `T+10s` of an attempt window.

## Security notes

- `/etc/tennis-booking/env` is `0640 root:whimpy` — only root and the service user can read it. Never `cat` it in a shared terminal; use `sudo -e` to edit.
- The bearer token is redacted from logs by a filter in `obs/logging.py` + `altegio/client.py` (covers httpx/httpcore too).
- `profiles.yaml` contains PII (real names, phones). Mode `0640`. Do **not** commit a populated copy — `.gitignore` already excludes `config/profiles.yaml` and `config/schedule.yaml` at repo level.
- The deploy key is dedicated to this repo and server. Revoke by removing the public key from `/home/whimpy/.ssh/authorized_keys` if it ever leaks.
- GitHub repository secrets are scoped to Actions; they are not exposed in logs unless deliberately echoed. The deploy workflow never echoes secrets.
