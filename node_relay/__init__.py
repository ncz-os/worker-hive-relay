"""node-relay: E2EE cloud-object relay for network-isolated remote workers.

Bridges a remote or airgapped node (internet egress only, no LAN/VPN to the
home fleet) to the GRAEAE hive bus through a public-cloud object store (GCS or
S3). Two stateless pollers, one bucket, no on-prem bridge host:

    enqueuer (home)    -> seal -> bucket pending/
    node_poller (node) -> claim/execute -> bucket terminal/
    reconciler (home)  -> open -> land patch -> hive done/failed -> purge

See ``README.md`` and ``docs/NODE_RELAY_BRIDGE.md`` for the architecture.
"""

__all__ = ["relay_crypto", "relay_client"]
