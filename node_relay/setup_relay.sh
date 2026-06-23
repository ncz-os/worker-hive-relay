#!/usr/bin/env bash
# node-relay idempotent bootstrap. Safe to run repeatedly: it never clobbers an
# existing secret, never re-creates an existing bucket, and only (re)installs a
# systemd unit when its content actually changed.
#
# Usage:
#   ./setup_relay.sh home       # install enqueuer + reconciler (home fleet)
#   ./setup_relay.sh node       # install poller (remote/airgapped node)
#   ./setup_relay.sh env-only   # just create the EnvironmentFile + key
#
# Designed for remote/airgapped nodes that reach a public cloud bucket (GCS or
# S3) but have no LAN/VPN to the home fleet. Nothing here involves any specific
# hardware vendor.
set -euo pipefail

ROLE="${1:-env-only}"
CFG_DIR="${NODE_RELAY_CONFIG_DIR:-$HOME/.config/node-relay}"
ENV_FILE="$CFG_DIR/relay.env"
UNIT_SRC="$(cd "$(dirname "$0")/ops" && pwd)"
PY="${PYTHON:-python3}"

log() { printf '[node-relay setup] %s\n' "$*"; }

ensure_env_file() {
  mkdir -p "$CFG_DIR"
  chmod 700 "$CFG_DIR"
  if [ ! -f "$ENV_FILE" ]; then
    log "creating $ENV_FILE (fill in the blanks; key generated below)"
    umask 077
    cat > "$ENV_FILE" <<'EOF'
# node-relay environment (mode 0600). Secrets never go in the bucket or git.
# --- both sides ---
NODE_RELAY_E2EE_KEY=
# Bucket: bare name, gs://name, or s3://name. Backend inferred from scheme,
# or set NODE_RELAY_BACKEND=gcs|s3 explicitly.
NODE_RELAY_BUCKET=
# NODE_RELAY_BACKEND=gcs
# --- GCP backend ---
# NODE_RELAY_GCS_SA=/home/USER/.config/node-relay/relay-sa.json
# --- AWS / S3-compatible backend ---
# AWS_REGION=us-east-1
# (AWS creds via env / shared config / instance role; or:)
# NODE_RELAY_S3_ENDPOINT=https://s3.example.com   # only for non-AWS S3
# --- home side only ---
# NODE_RELAY_HOST=<remote node host id used as eligible_hosts>
# HIVE_BASE=http://192.168.207.67:5005
# HIVE_BUS_TOKEN=
# MNEMOS_BASE=http://192.168.207.67:5002
# MNEMOS_TOKEN=
# --- node side only (optional cloud LLM fallback; local is primary) ---
# LLM_BASE=http://localhost:11434/v1
# CLOUD_LLM_BASE=
# CLOUD_LLM_API_KEY=
# CLOUD_LLM_MODEL=
EOF
    chmod 600 "$ENV_FILE"
  else
    log "$ENV_FILE already exists — leaving it untouched"
  fi
}

ensure_key() {
  # Generate a 32-byte AES key ONLY if one is not already set. Idempotent: a
  # second run sees the key and does nothing (so both sides keep the SAME key).
  if grep -qE '^NODE_RELAY_E2EE_KEY=.+' "$ENV_FILE"; then
    log "E2EE key already present — not regenerating"
    return
  fi
  local key
  key="$($PY - <<'EOF'
import base64, os
print(base64.b64encode(os.urandom(32)).decode())
EOF
)"
  # Replace the empty assignment in place.
  tmp="$(mktemp)"
  sed "s|^NODE_RELAY_E2EE_KEY=.*|NODE_RELAY_E2EE_KEY=$key|" "$ENV_FILE" > "$tmp"
  cat "$tmp" > "$ENV_FILE"
  rm -f "$tmp"
  chmod 600 "$ENV_FILE"
  log "generated NODE_RELAY_E2EE_KEY — COPY THIS SAME VALUE to the other side:"
  log "  $key"
}

ensure_bucket() {
  # Best-effort, idempotent: create the bucket only if it does not exist.
  # Requires the relevant SDK + credentials in the environment; skips with a
  # note otherwise (provisioning can also be done out of band).
  # shellcheck disable=SC1090
  set -a; . "$ENV_FILE"; set +a
  [ -n "${NODE_RELAY_BUCKET:-}" ] || { log "NODE_RELAY_BUCKET unset — skipping bucket check"; return; }
  NODE_RELAY_BUCKET="$NODE_RELAY_BUCKET" NODE_RELAY_BACKEND="${NODE_RELAY_BACKEND:-}" "$PY" - <<'EOF' || log "bucket check skipped (SDK/creds unavailable)"
import os, sys
raw = os.environ["NODE_RELAY_BUCKET"]
backend = (os.environ.get("NODE_RELAY_BACKEND") or "").lower()
for scheme, name in (("gs://", "gcs"), ("s3://", "s3")):
    if raw.startswith(scheme):
        backend = backend or name
        raw = raw[len(scheme):]
backend = backend or "gcs"
if backend == "gcs":
    from google.cloud import storage
    c = storage.Client()
    if c.lookup_bucket(raw) is None:
        c.create_bucket(raw)
        print(f"created GCS bucket {raw}")
    else:
        print(f"GCS bucket {raw} already exists")
else:
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3", region_name=os.environ.get("NODE_RELAY_S3_REGION") or os.environ.get("AWS_REGION"))
    try:
        s3.head_bucket(Bucket=raw)
        print(f"S3 bucket {raw} already exists")
    except ClientError:
        s3.create_bucket(Bucket=raw)
        print(f"created S3 bucket {raw}")
EOF
}

install_unit() {
  # Idempotent system-unit install: copy only if changed, reload, enable+start.
  local unit="$1"
  local src="$UNIT_SRC/$unit"
  local dst="/etc/systemd/system/$unit"
  [ -f "$src" ] || { log "missing unit $src"; return 1; }
  if [ -f "$dst" ] && cmp -s "$src" "$dst"; then
    log "$unit unchanged"
  else
    log "installing $unit"
    sudo cp "$src" "$dst"
    sudo systemctl daemon-reload
  fi
  sudo systemctl enable --now "$unit"
}

ensure_env_file
ensure_key
case "$ROLE" in
  home)
    ensure_bucket
    install_unit node-relay-enqueuer.service
    install_unit node-relay-reconciler.service
    ;;
  node)
    ensure_bucket
    install_unit node-relay-poller.service
    ;;
  env-only)
    log "env prepared; re-run with 'home' or 'node' to install units"
    ;;
  *)
    log "unknown role '$ROLE' (want: home | node | env-only)"; exit 2 ;;
esac
log "done."
