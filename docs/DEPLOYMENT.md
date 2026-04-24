# Deployment

Production deployment of `tennis-booking` to `194.195.241.83` (user `whimpy`, SSH port `13022`).

The service runs as a Docker container managed by systemd, mirroring the deployment shape of the `telegram-bot-dev` service that already lives on the same host.

## Architecture

```
┌─────────────────┐  push to main   ┌──────────────────────┐
│   GitHub repo   │ ──────────────▶ │  GitHub Actions      │
└─────────────────┘                 │  build → ghcr.io     │
                                    │  ssh → docker pull   │
                                    │  ssh → systemctl restart
                                    └──────────┬───────────┘
                                               │ ssh (key auth)
                                               ▼
                                    ┌──────────────────────┐
                                    │  whimpy@194.195...   │
                                    │  systemd unit wraps  │
                                    │  `docker run` of     │
                                    │  ghcr.io/.../tennis- │
                                    │   booking:latest     │
                                    └──────────┬───────────┘
                                               │ bind mounts
                          ┌────────────────────┼────────────────────┐
                          ▼                    ▼                    ▼
                /var/lib/tennis-       /var/lib/tennis-     /var/lib/tennis-
                  booking/config         booking/logs         booking/data
                  (ro: YAMLs)            (rw: service.log)    (rw: future SQLite)
```

Key paths on the server:

| Path                                        | Purpose                              | Owner         | Mode |
| ------------------------------------------- | ------------------------------------ | ------------- | ---- |
| `/etc/tennis-booking/env`                   | EnvironmentFile (bearer token etc.)  | root:whimpy   | 0640 |
| `/var/lib/tennis-booking/config/schedule.yaml`  | Booking schedule (read-only mount) | root:whimpy   | 0640 |
| `/var/lib/tennis-booking/config/profiles.yaml`  | Client profiles, PII (read-only)   | root:whimpy   | 0640 |
| `/var/lib/tennis-booking/logs/`             | RotatingFileHandler sink             | 1000:1000     | 0750 |
| `/var/lib/tennis-booking/data/`             | Future SQLite                        | 1000:1000     | 0750 |
| `/etc/systemd/system/tennis-booking.service`| Unit file (wraps docker run)         | root:root     | 0644 |
| `/etc/sudoers.d/tennis-booking`             | NOPASSWD entry for docker/systemctl  | root:root     | 0440 |
| `/opt/tennis-booking/`                      | Optional repo checkout (convenience) | whimpy:whimpy | 0755 |

The container itself runs as uid 1000 (`app` user inside the image). On this host `whimpy` is also uid 1000, so file ownership lines up naturally.

## Prerequisites

Server (Ubuntu / Debian assumed):

- Docker Engine — `apt install docker.io` or follow https://docs.docker.com/engine/install/
- systemd, OpenSSH server
- Time sync: `chrony` (preferred) or `systemd-timesyncd`
- Outbound firewall:
  - TCP 443 → `ghcr.io` (image pull)
  - TCP 443 → `b551098.alteg.io` (Altegio API)
  - UDP 123 → `pool.ntp.org` (in-app NTP drift check)

`whimpy` is **not** added to the `docker` group. All docker commands go through `sudo`, gated by a narrow sudoers entry. This keeps the deploy user from holding root-equivalent privileges between deploys.

Workstation:

- `ssh-keygen`, `ssh`, `ssh-copy-id`
- Access to the GitHub repository **Settings → Secrets and variables → Actions**

## One-time setup

### 1. GitHub repository settings

Repository → **Settings → Actions → General → Workflow permissions**:

- Select **Read and write permissions** (so the build job's `GITHUB_TOKEN` can push to ghcr.io).
- Tick **Allow GitHub Actions to create and approve pull requests** (not strictly required for deploy, but harmless and recommended).

### 2. Generate a deploy SSH keypair (on your workstation)

Dedicated key — do **not** reuse your personal SSH key.

```bash
ssh-keygen -t ed25519 -f ~/.ssh/tennis-booking-deploy -N "" \
    -C "github-actions-deploy@tennis-booking"
```

This creates:

- `~/.ssh/tennis-booking-deploy` (private — goes into GitHub secret)
- `~/.ssh/tennis-booking-deploy.pub` (public — goes on the server)

### 3. Install the public key on the server

```bash
ssh-copy-id -i ~/.ssh/tennis-booking-deploy.pub -p 13022 whimpy@194.195.241.83
```

Smoke-test:

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
| `DEPLOY_HOST_KEY` | Output of `ssh-keyscan -p 13022 194.195.241.83`            |
| `DEPLOY_HOST`     | `194.195.241.83`                                           |
| `DEPLOY_PORT`     | `13022`                                                    |
| `DEPLOY_USER`     | `whimpy`                                                   |

`GITHUB_TOKEN` is provisioned automatically per workflow run — no secret to create.

### 6. Bootstrap the server

SSH to the server and run the setup script. It is idempotent, safe to re-run.

```bash
ssh -p 13022 whimpy@194.195.241.83

# Install Docker + chrony if not already there
sudo apt update
sudo apt install -y docker.io chrony

# Fetch and run the setup script (cleans up after itself)
curl -sLO https://raw.githubusercontent.com/RomanGoltsov/tennis-booking/main/deploy/setup-server.sh
chmod +x setup-server.sh
sudo ./setup-server.sh
```

The script:

- Verifies docker, chrony, and the `whimpy` user exist
- Creates `/etc/tennis-booking/` and `/var/lib/tennis-booking/{config,logs,data}` with correct ownership
- Optionally clones the repo to `/opt/tennis-booking` for unit/sudoers convenience (NOT used at runtime)
- Installs the systemd unit and enables it (does NOT start it)
- Drops an env file stub at `/etc/tennis-booking/env` if missing

### 7. Populate secrets and config on the server

```bash
# 7a. Bearer token
sudo -e /etc/tennis-booking/env
# Edit ALTEGIO_BEARER_TOKEN=...   (value captured from Phase 0 DevTools research)
# Leave ALTEGIO_DRY_RUN commented (defaults to false = real bookings).

# 7b. Schedule + profiles (read-only mount → /app/config inside container)
sudo -e /var/lib/tennis-booking/config/schedule.yaml
sudo -e /var/lib/tennis-booking/config/profiles.yaml
```

File modes should already be correct from setup-server.sh. Confirm:

```bash
ls -l /etc/tennis-booking/ /var/lib/tennis-booking/config/
# /etc/tennis-booking/:
#   -rw-r----- root whimpy env
# /var/lib/tennis-booking/config/:
#   -rw-r----- root whimpy schedule.yaml
#   -rw-r----- root whimpy profiles.yaml
```

### 8. Install the sudoers entry

The GitHub Actions workflow invokes `sudo docker pull` and `sudo systemctl restart` on the server; `whimpy` needs NOPASSWD privilege for exactly those commands.

```bash
sudo install -o root -g root -m 0440 \
    /opt/tennis-booking/deploy/tennis-booking.sudoers \
    /etc/sudoers.d/tennis-booking

# Validate — never edit sudoers without visudo's syntax check
sudo visudo -cf /etc/sudoers.d/tennis-booking
# → "parsed OK"
```

If `visudo -c` fails, `rm /etc/sudoers.d/tennis-booking` immediately; a broken sudoers file can lock you out.

If `/opt/tennis-booking` doesn't exist (you skipped the optional checkout), download the file directly:

```bash
sudo curl -fsSL -o /etc/sudoers.d/tennis-booking \
    https://raw.githubusercontent.com/RomanGoltsov/tennis-booking/main/deploy/tennis-booking.sudoers
sudo chmod 0440 /etc/sudoers.d/tennis-booking
sudo visudo -cf /etc/sudoers.d/tennis-booking
```

### 9. Make the container image public (after first build)

The first push to `main` triggers `.github/workflows/deploy.yml`. The `build-and-push` job creates a package at:

```
ghcr.io/romer533/tennis-booking
```

By default GHCR packages are **private** — the server's `sudo docker pull` will get `denied: requested access to the resource is denied` because the unauthenticated daemon can't see it.

Fix once: GitHub → your profile → **Packages → tennis-booking → Package settings → Change visibility → Public**.

Alternatively, keep the package private and add a `GHCR_PULL_TOKEN` (a PAT with `read:packages`) plus a `docker login ghcr.io` step on the server. Public is simpler and the image contains no secrets.

After flipping visibility, retry the deploy: **Actions → deploy → Run workflow**.

### 10. Verify NTP

Offset must be < 50 ms — the scheduler's `clock.py` NTP check enforces this at startup (`ntp_threshold_ms=50`).

```bash
chronyc tracking | grep -E "Leap|Last offset|System time"
# Leap status     : Normal
# System time     : 0.00001234 seconds slow of NTP time   ← |offset| < 0.050
```

If drift is high: `sudo systemctl restart chrony && sleep 30 && chronyc tracking`.

### 11. First manual start

```bash
# Pull the latest image manually for the first start (subsequent restarts pull via workflow)
sudo docker pull ghcr.io/romer533/tennis-booking:latest

sudo systemctl start tennis-booking
sudo systemctl status tennis-booking --no-pager
```

Expected: `Active: active (running)`. If it's `activating (auto-restart)` — check logs:

```bash
sudo journalctl -u tennis-booking -n 50 --no-pager
sudo docker logs tennis-booking --tail 50    # also useful while the unit is bouncing
```

### 12. Live monitoring

Two equivalent views; keep both handy during the first week:

```bash
# systemd journal — captures container stdout/stderr (incl. crash trace)
sudo journalctl -u tennis-booking -f

# structlog JSON file — survives systemd cycling, grep-friendly
sudo tail -f /var/lib/tennis-booking/logs/service.log
```

## Continuous deployment

After the one-time setup, every `git push origin main` triggers `.github/workflows/deploy.yml`:

1. **build-and-push** — Buildx builds the image with GHA cache and pushes to `ghcr.io/romer533/tennis-booking:latest` plus `:sha-<short>`.
2. **deploy** (depends on build) — SSHes to the server and runs:
   - `sudo docker pull ghcr.io/romer533/tennis-booking:latest`
   - `sudo systemctl restart tennis-booking`
   - polls `systemctl is-active` for up to 8s
   - dumps last 30 journal lines on success / 50 on failure

Concurrency is `deploy-production` with `cancel-in-progress: false` — a second push queues behind the first rather than replacing it.

Manual trigger: repository → **Actions → deploy → Run workflow**.

## Rollback

Each successful build is tagged `:sha-<short>`. To roll back to a known-good build:

```bash
ssh -p 13022 whimpy@194.195.241.83

# Identify the previous good SHA (e.g. from the deploy log or `git log`)
sudo docker pull ghcr.io/romer533/tennis-booking:sha-abc1234

# Re-tag locally so the unit (which references :latest) picks it up:
sudo docker tag ghcr.io/romer533/tennis-booking:sha-abc1234 \
                 ghcr.io/romer533/tennis-booking:latest
sudo systemctl restart tennis-booking
sudo systemctl is-active tennis-booking
sudo journalctl -u tennis-booking -n 30 --no-pager
```

The next push to `main` will overwrite `:latest` again — so follow up with a `git revert` of the bad commit and push, otherwise the next deploy reapplies the broken code.

## Troubleshooting

### `denied: requested access to the resource is denied` on `docker pull`

The GHCR package is still private. See **Step 9** above — flip visibility to Public. If keeping it private: ensure the host has run `docker login ghcr.io` with a PAT having `read:packages`.

### `ClockDriftError: drift 123ms > threshold 50ms`

NTP is lagging. `chronyc tracking` → check `Last offset`, `System time`. Fix:

```bash
sudo systemctl restart chrony
sleep 30
chronyc sources -v
chronyc tracking
```

Temporary workaround: set `TENNIS_NTP_REQUIRED=0` in `/etc/tennis-booking/env`, then `sudo systemctl restart tennis-booking`.

### `AltegioConfigError: set ALTEGIO_BEARER_TOKEN env var`

`/etc/tennis-booking/env` is missing or empty. Verify:

```bash
sudo cat /etc/tennis-booking/env
# Must contain a non-empty line: ALTEGIO_BEARER_TOKEN=eyJ...

sudo systemctl cat tennis-booking | grep env-file
# --env-file /etc/tennis-booking/env
```

### `ConfigError: schedule.yaml not found`

```bash
sudo ls -l /var/lib/tennis-booking/config/
# Both schedule.yaml and profiles.yaml must be present (mounted read-only at /app/config).
```

### `sudo: a password is required` (in GitHub Actions log)

Sudoers entry missing or not parsed. Reinstall (see Step 8) and validate with `sudo visudo -cf /etc/sudoers.d/tennis-booking`.

### `Permission denied (publickey)` (in GitHub Actions log)

- `DEPLOY_SSH_KEY` secret was pasted with wrong line endings. Re-copy the full file including `-----BEGIN OPENSSH PRIVATE KEY-----` header/footer.
- Public key not installed on server: `ssh whimpy@... 'cat .ssh/authorized_keys' | grep github-actions-deploy`.

### Log file not rotating

```bash
sudo ls -lh /var/lib/tennis-booking/logs/
# Expect: service.log and, after 10 MB, service.log.1, service.log.2, ...
# backupCount=14, so max 15 files total (current + 14 old).
```

If only `service.log` grows past 10 MB, the handler didn't install — check container stdout for a `cannot set up logging` error. Most likely the host directory is owned by something other than 1000:1000.

### Service dies immediately on start

```bash
sudo systemctl status tennis-booking --no-pager
sudo journalctl -u tennis-booking -n 100 --no-pager
sudo docker logs tennis-booking --tail 100   # if the container is still around
```

Most common root causes are listed above (env, configs, NTP, image pull). If none match, paste the last 100 lines into an issue.

### How to stop the service temporarily

```bash
sudo systemctl stop tennis-booking          # stop, keep enabled (starts on boot)
sudo systemctl disable --now tennis-booking # stop, don't start on boot
```

`TimeoutStopSec=70` in the unit + `docker stop -t 60` in `ExecStop` together give the in-flight booking attempt up to 60s to finish before SIGKILL. Do **not** `kill -9` the container between `T-30s` and `T+10s` of an attempt window.

## Security notes

- `/etc/tennis-booking/env` is `0640 root:whimpy` — only root and the service user can read it. Never `cat` it in a shared terminal; use `sudo -e` to edit.
- The bearer token is redacted from logs by a filter in `obs/logging.py` + `altegio/client.py` (covers httpx/httpcore too).
- `profiles.yaml` contains PII (real names, phones). Mode `0640`. Do **not** commit a populated copy — `.gitignore` already excludes `config/profiles.yaml` and `config/schedule.yaml` at repo level; the host copies live in `/var/lib/tennis-booking/config/`.
- The container runs as non-root (uid 1000) inside its own filesystem namespace. Bind mounts are read-only for `/app/config` and read-write for `/app/logs` and `/app/data` only.
- The deploy key is dedicated to this repo and server. Revoke by removing the public key from `/home/whimpy/.ssh/authorized_keys` if it ever leaks.
- The sudoers entry whitelists exactly: `docker pull ghcr.io/romer533/tennis-booking:*`, `systemctl {restart,is-active,status,show} tennis-booking`, `journalctl -u tennis-booking *`. No shell escape, no general docker access.
- GitHub repository secrets are scoped to Actions; they are not exposed in logs unless deliberately echoed. The deploy workflow never echoes secrets.
- The container image is published to ghcr.io and intended to be **public** for simplicity — it contains no secrets, only application code. If you flip it to private, add a PAT-based `docker login` step on the server.
