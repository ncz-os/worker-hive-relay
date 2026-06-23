# worker-hive-relay

E2EE public-cloud object relay bridging **network-isolated remote workers** to
the GRAEAE hive bus. It works for any node that has internet egress but **no LAN
or VPN path** to the home fleet — airgapped boxes, corp-locked hardware, or a
cloud VM in another network. A **GCS or S3 bucket** is the transport; payloads
are AES-256-GCM sealed end-to-end, so the cloud sees only ciphertext. No vendor
hardware, NGC, or specific GPU is required.

```
hive job [eligible_hosts=<node>]
  → enqueuer (home): claim + MNEMOS context-prepackage + seal → bucket pending/
  → poller (node):   atomic-claim + decrypt + local/cloud LLM exec → terminal/
  → reconciler (home): land review branch + PATCH hive done/failed + purge
```

The Python package is [`node_relay/`](node_relay/) — see
[`node_relay/README.md`](node_relay/README.md) for the module map, install,
backend selection, and the bucket/lease/E2EE design. Ops units +
idempotent bootstrap live in [`node_relay/ops/`](node_relay/ops/) and
[`node_relay/setup_relay.sh`](node_relay/setup_relay.sh).

## Why a bucket?

Routing is by `eligible_hosts`; the atomic claim is a conditional create
(GCS `ifGenerationMatch=0` / S3 `If-None-Match: *`); the terminal state is a
single create-only object. Two stateless pollers, one bucket, no on-prem bridge
host and no inbound path to the home network. Either side can be offline and
jobs queue safely.

## Two clouds, one interface

Select the backend by bucket URL scheme (`gs://…` or `s3://…`) or
`NODE_RELAY_BACKEND=gcs|s3`. Install only the SDK you use
(`pip install '.[gcs]'` or `'.[s3]'`).

## Migration note

This was previously `spark_relay` (built for one vendor's network-isolated
box). It has been generalized to any remote/airgapped node and renamed to
`node_relay`. Every renamed environment variable falls back to its legacy
`SPARK_*` name, and the crypto reader accepts blobs sealed with the old magic,
so an in-place upgrade needs no flag-day cutover.
