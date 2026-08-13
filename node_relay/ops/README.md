# node-relay — systemd units

Three units. Two run on the **home fleet** (enqueuer + reconciler), one on the
**remote node** (poller). All read secrets from an `EnvironmentFile` so nothing
sensitive lives in the unit or the repo.

`node-relay-home.service` is an optional replacement for both home units. It
runs the roles in separate threads under one Python interpreter. Disable the
enqueuer and reconciler units before enabling it; the bootstrap intentionally
keeps installing the existing separate units by default.

The quickest path is the idempotent bootstrap (creates the env file + key,
optionally the bucket, and installs only the changed units):

```bash
./node_relay/setup_relay.sh home    # home fleet
./node_relay/setup_relay.sh node    # remote node
```

## EnvironmentFile (manual)

Create `~/.config/node-relay/relay.env` (mode 0600) on each host:

```ini
# both sides — identical key on every participant
NODE_RELAY_E2EE_KEY=<base64-32-bytes>
NODE_RELAY_BUCKET=gs://my-relay-bkt      # or s3://my-relay-bkt or a bare name
# NODE_RELAY_BACKEND=gcs|s3              # optional; inferred from the scheme

# GCP backend
NODE_RELAY_GCS_SA=/home/<user>/.config/node-relay/relay-sa.json
# AWS / S3-compatible backend
# AWS_REGION=us-east-1                   # creds via env / shared config / role
# NODE_RELAY_S3_ENDPOINT=https://...     # only for non-AWS S3

# home only
NODE_RELAY_HOST=<remote node host id>    # the eligible_hosts value jobs target
HIVE_BASE=http://192.168.207.67:5005
HIVE_BUS_TOKEN=<bus bearer>
MNEMOS_BASE=http://192.168.207.67:5002
MNEMOS_TOKEN=<mnemos bearer>

# node only (local model is primary; cloud is an optional fallback — no vendor default)
# LLM_BASE=http://localhost:11434/v1
# CLOUD_LLM_BASE=https://api.example.com/v1
# CLOUD_LLM_API_KEY=<key>
# CLOUD_LLM_MODEL=<model>
```

## Install (manual)

The `.service` files contain `@NODE_RELAY_*@` placeholders. Replace each with
the desired user, checkout, environment-file, and Python paths before copying,
or use the bootstrap above to render them automatically.

```bash
sudo cp node-relay-enqueuer.service node-relay-reconciler.service /etc/systemd/system/  # home
sudo cp node-relay-poller.service /etc/systemd/system/                                  # node
sudo systemctl daemon-reload
sudo systemctl enable --now node-relay-enqueuer node-relay-reconciler                    # home
sudo systemctl enable --now node-relay-poller                                            # node
```

To opt into the shared home runtime, render and install
`node-relay-home.service`, then switch units explicitly:

```bash
sudo systemctl disable --now node-relay-enqueuer node-relay-reconciler
sudo systemctl enable --now node-relay-home
```

The checked-in units are templates. The bootstrap fills in the invoking user,
checkout, environment file, and active virtual environment (or the checkout's
`.venv`). Set `NODE_RELAY_SERVICE_USER` or `NODE_RELAY_SERVICE_PYTHON` to
override those values. Re-running `setup_relay.sh` is safe: it reinstalls a unit
only when its rendered content changed, then reloads and enables it.

## Backward compatibility

Hosts still carrying the old `SPARK_HIVE_RELAY_*` / `SPARK_*` environment
variables keep working — every renamed variable falls back to its legacy name,
and the crypto reader accepts in-flight blobs sealed with the old magic. Migrate
at your leisure by switching the EnvironmentFile to the `NODE_RELAY_*` names.
