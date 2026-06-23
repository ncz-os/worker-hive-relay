# node-relay — E2EE public-cloud bridge for network-isolated workers

Integrates any **remote or airgapped node** as a hive worker even though it
**cannot reach the home fleet** (no LAN, no VPN). The only requirement is that
both sides can reach a **public-cloud object store** — Google Cloud Storage or
Amazon S3 (or any S3-compatible endpoint). That bucket is the transport;
payloads are end-to-end encrypted (AES-256-GCM), so the cloud sees only
ciphertext.

Full design + rationale: [`../docs/NODE_RELAY_BRIDGE.md`](../docs/NODE_RELAY_BRIDGE.md)
(in the mnemos-core docs tree).

```
enqueuer (home)    hive job → MNEMOS context → seal → bucket pending/
node_poller (node) list pending → atomic-claim → local/cloud exec → seal → bucket terminal/
reconciler (home)  poll terminal/ → open → LAND patch as hive/node-<id> → hive done/failed → purge
```

The atomic claim is a conditional create — GCS `ifGenerationMatch=0` or S3
`If-None-Match: *` on `claimed/<uuid>` — so exactly one worker wins with no lock
server. The single terminal object per job is likewise create-only.

## Modules

| File | Side | Role |
|------|------|------|
| `relay_crypto.py` | both | AES-256-GCM seal/open (`NDR1` framed, header as AAD; reads legacy `SHR1`) |
| `relay_client.py` | both | GCS **or** S3 transport behind one interface: put/list/**claim**/get/purge |
| `bridge_common.py` | home | hive + MNEMOS HTTP clients, backoff |
| `enqueuer.py` | home | drain eligible hive jobs → bucket |
| `reconciler.py` | home | bucket terminal → land review branch → hive status |
| `lander.py` | home | apply a node patch and push `hive/node-<jobid>` (idempotent) |
| `node_poller.py` | node | claim + execute via pluggable `Executor` (local or cloud LLM) |

## Install

```bash
pip install -r node_relay/requirements.txt        # base
pip install 'google-cloud-storage>=2.16.0'         # GCP backend
# or
pip install 'boto3>=1.34.0'                        # AWS / S3 backend
```

Or, from a checkout: `pip install '.[gcs]'` / `pip install '.[s3]'`.

## Configure (idempotent bootstrap)

```bash
./node_relay/setup_relay.sh home    # home fleet: env + key + enqueuer/reconciler
./node_relay/setup_relay.sh node    # remote node: env + key + poller
```

Re-running is safe — it never clobbers an existing key/env and only reinstalls a
unit whose content changed. See [`ops/README.md`](ops/README.md) for the full
EnvironmentFile reference.

## Secrets (never in the bucket, never committed)

| Env var | Meaning |
|---------|---------|
| `NODE_RELAY_E2EE_KEY` | base64 AES-256 key — **identical** on every participant |
| `NODE_RELAY_BUCKET` | `gs://name`, `s3://name`, or a bare name (+ `NODE_RELAY_BACKEND`) |
| `NODE_RELAY_GCS_SA` | path to the GCS service-account JSON (GCP backend) |
| `AWS_*` / `NODE_RELAY_S3_*` | standard S3 credentials/region/endpoint (S3 backend) |
| `NODE_RELAY_HOST` (home) | the remote node's host id (the `eligible_hosts` jobs target) |
| `MNEMOS_TOKEN` (home) | MNEMOS bearer for context retrieval |
| `CLOUD_LLM_*` (node) | optional hosted OpenAI-compatible fallback model |

Every `NODE_RELAY_*` variable falls back to its legacy `SPARK_*` /
`SPARK_HIVE_RELAY_*` name, so un-migrated hosts keep working.

## Run (manual)

```bash
# home
python -m node_relay.enqueuer   --interval 15
python -m node_relay.reconciler --interval 15
# remote node
python -m node_relay.node_poller --interval 10 --executor local+cloud
```

`--once` does a single sweep (useful for cron or smoke tests). The poller's
`--executor` is `local | cloud | local+cloud | cloud+local | auto`; the cloud
fallback is a generic OpenAI-compatible endpoint (`CLOUD_LLM_*`) — no vendor
lock-in.

## Executor integration

`node_poller` ships an `OpenAIChatExecutor` (chat) and an `AgenticRepoExecutor`
(clone → request complete replacement files → local review commit → format-patch
through the bucket; the home-side `lander` pushes the review branch). The node
never holds fleet git credentials. Implement `Executor.execute` and wire it in
`make_executor` to swap runtimes.

## Tests

```bash
pytest tests/node_relay/        # pure: crypto, queue flow, backend select, repo resolution
```

The flow tests run against an in-memory backend with the same conditional-create
/ CAS semantics as GCS and S3, so the exactly-once and idempotency guarantees are
verified without any network.
