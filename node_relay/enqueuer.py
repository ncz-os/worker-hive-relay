"""Home-side enqueuer: hive job -> context-prepackage -> seal -> pending/.

Runs as a loop on a home-fleet host that reaches both the hive bus and MNEMOS.
For each job it claims on the hive that is eligible for the remote node, it
pulls relevant MNEMOS context (the node is stateless / network-isolated), seals
the combined payload with the shared E2EE key, and drops it in the relay
bucket's ``pending/`` prefix. The node poller takes it from there.

Idempotency: the hive job id IS the relay uuid, and the node claim is a
conditional create, so re-enqueuing the same job is harmless.
"""

from __future__ import annotations

import argparse
import logging
import os
import time

from . import relay_crypto
from .bridge_common import HiveClaimError, HiveClient, backoff_sleep, mnemos_get, mnemos_search
from .node_poller import NONCOMMIT_PREFIXES, repo_url_for_job, repo_url_for_kind
from .relay_client import RelayClient

log = logging.getLogger("node_relay.enqueuer")


def _env(*names: str, default: str | None = None) -> str | None:
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return default


# The remote node's host id. The enqueuer registers AS this host so the hive
# offers it jobs submitted with eligible_hosts=[<node-host>]. No vendor default:
# operators set NODE_RELAY_HOST per node. (Legacy SPARK_GPU_HOST still honored.)
NODE_HOST = _env("NODE_RELAY_HOST", "SPARK_GPU_HOST")
CONTEXT_LIMIT = 6
HIVE_LEASE_SECONDS = float(_env("NODE_RELAY_HIVE_LEASE_SECONDS", default="1800"))


def _require_host() -> str:
    if not NODE_HOST:
        raise SystemExit(
            "set $NODE_RELAY_HOST to the remote node's host id "
            "(the eligible_hosts value home-fleet jobs target)"
        )
    return NODE_HOST


def build_payload(job: dict) -> dict:
    """Shape the sealed job the node will execute. Pre-packages MNEMOS context."""
    prompt = job.get("prompt") or job.get("task") or job.get("description", "")
    exact_context = [mnemos_get(memory_id) for memory_id in (job.get("mnemos_refs") or [])]
    exact_ids = {memory.get("id") for memory in exact_context}
    semantic_context = [
        memory
        for memory in mnemos_search(prompt, limit=CONTEXT_LIMIT)
        if memory.get("id") not in exact_ids
    ]
    context = exact_context + semantic_context
    claimed_resources = job.get("claimed_resources") or {}
    claimed_model = claimed_resources.get("model") if isinstance(claimed_resources, dict) else None
    execution_model = claimed_model if claimed_model not in (None, "", "unknown") else None
    return {
        "job_id": job["id"],
        "kind": job.get("kind"),  # poller KIND_WORKSPACE_MAP needs it for repo mapping
        "prompt": prompt,
        # Use only a model Hive actually selected. ``preferred_models`` is a
        # scheduling ranking, not an execution requirement; unresolved relay
        # claims use the node executor's configured default model.
        "model": execution_model,
        # Both supported Hive variants persist and dequeue ``project``. Treat
        # it as the cross-version structured workspace identifier; the newer
        # target_workspace name is accepted when a Hive begins forwarding it.
        "workspace": job.get("target_workspace") or job.get("project"),
        # Backward compatibility for relay producers predating workspace.
        "repo": job.get("repo"),
        "branch": job.get("branch"),
        "context": context,
        "meta": {"submitter": job.get("submitter_urn"), "priority": job.get("priority")},
    }


def _node_should_offload(job: dict) -> bool:
    """The node only takes work it can actually complete.

    Offload no-commit work without a repository. Commit-producing work must
    resolve through the node's allowlist, including when it is explicitly
    host-targeted. Everything else is left for the home fleet, which has the
    full repo map. This prevents the node from claiming build jobs it cannot
    finish — they would otherwise zombie in 'running' or degrade to a useless
    chat suggestion.
    """
    host = _require_host().lower()
    kind = str(job.get("kind") or "")
    eligible_hosts = job.get("eligible_hosts") or []
    if kind.startswith(NONCOMMIT_PREFIXES):
        return True
    if repo_url_for_kind(kind) is not None:
        return True

    # Host targeting controls placement, but cannot supply repository identity.
    # Older Hive producers discard target_workspace and emit only project, so a
    # host-targeted commit job is safe to claim only when its emitted structured
    # workspace resolves locally. Otherwise release it for a worker with a full
    # repository map instead of sealing an unexecutable job.
    host_targeted = any(str(h).strip().lower() == host for h in eligible_hosts)
    return host_targeted and repo_url_for_job(job) is not None


def run_once(hive: HiveClient, relay: RelayClient, key: bytes) -> int:
    """Drain the hive of eligible jobs into the bucket. Returns count enqueued."""
    enqueued = 0
    released_this_sweep: set[str] = set()
    while True:
        job = hive.claim_next()
        if job is None:
            break
        job_id = job["id"]
        if not _node_should_offload(job):
            # Release back to the queue (we are the claimant, so patch_status
            # defaults claimed_by to our URN) for a home-fleet worker to take.
            hive.patch_status(
                job_id,
                "queued",
                result={"note": "released by node-relay enqueuer: not node-offloadable (no repo mapping)"},
            )
            log.info("released %s (kind=%s) — not node-offloadable", job_id, job.get("kind"))
            if job_id in released_this_sweep:
                # Re-claimed our own release before a home worker did; stop to
                # avoid a hot release/claim loop. Next sweep retries.
                break
            released_this_sweep.add(job_id)
            continue
        try:
            payload = build_payload(job)
            # Embed the claimant URN so the node echoes it back in the terminal
            # object; the reconciler needs it to PATCH as the job's claimant (the
            # hive has no GET-single-job endpoint to look it up).
            payload["claimant_urn"] = hive.urn
            agent_token = getattr(hive, "agent_token", None)
            if agent_token:
                payload["_claimant_token"] = agent_token
            claimant_route = {"claimant_urn": hive.urn}
            if agent_token:
                claimant_route["claimant_token"] = agent_token
            claimant = relay_crypto.seal(
                claimant_route,
                key,
                aad=relay_crypto.aad_for("claimant", job_id),
            )
            sealed = relay_crypto.seal(payload, key, aad=relay_crypto.aad_for("pending", job_id))
            # Keep routing independently authenticated so a corrupt pending
            # ciphertext can still be reported to the hive by its claimant.
            relay.put_claimant(job_id, claimant)
            relay.put_pending(job_id, sealed)
            hive.patch_status(job_id, "running", result={"note": "offloaded to node-relay bucket"})
            log.info("enqueued %s (context=%d)", job_id, len(payload["context"]))
            enqueued += 1
        except Exception as exc:  # noqa: BLE001 — surface to hive, keep draining
            log.exception("enqueue failed for %s", job_id)
            hive.patch_status(job_id, "failed", result={"error": f"enqueue: {exc}"})
    return enqueued


def _gpu_status_name() -> str:
    return _require_host() + "-gpu"


def read_gpu(relay: RelayClient, key: bytes) -> dict:
    """Read the remote node's GPU snapshot off the bucket (fail-soft)."""
    name = _gpu_status_name()
    try:
        raw = relay.get_status(name)
        if not raw:
            return {}
        return relay_crypto.open_blob(raw, key, aad=relay_crypto.aad_for("status", name))
    except Exception as exc:  # noqa: BLE001
        log.warning("read_gpu failed: %s", exc)
        return {}


def gpu_metadata(relay: RelayClient, key: bytes) -> dict:
    """Build agent metadata so /v1/hosts surfaces the remote node's GPU (if any)
    in the dashboard. Nodes with no GPU report empty lists."""
    snap = read_gpu(relay, key)
    return {
        "specs": {"gpus": snap.get("specs_gpus", [])},
        "load": {"gpus_runtime": snap.get("gpus_runtime", [])},
    }


def renew_active_jobs(hive: HiveClient, relay: RelayClient, key: bytes) -> int:
    """Refresh Hive claims for authenticated, recently active remote jobs.

    Activity lives in the bucket, so a restarted enqueuer can resume renewal
    for work that is still executing instead of relying on process memory.
    """
    renewed = 0
    now = time.time()
    for job_id in relay.list_pending():
        raw = relay.get_active(job_id)
        if raw is None:
            continue
        name = f"active/{job_id}"
        try:
            activity = relay_crypto.open_blob(
                raw, key, aad=relay_crypto.aad_for("status", name)
            )
            if activity.get("job_id") != job_id:
                raise ValueError("activity job_id does not match object name")
            updated_at = float(activity["updated_at"])
            claimant = activity["claimant_urn"]
            if not isinstance(claimant, str) or not claimant:
                raise ValueError("activity has no claimant_urn")
            claimant_token = activity.get("claimant_token")
            if claimant_token is not None and (
                not isinstance(claimant_token, str) or not claimant_token
            ):
                raise ValueError("activity has an invalid claimant_token")
        except (relay_crypto.RelayCryptoError, KeyError, TypeError, ValueError) as exc:
            log.warning("invalid activity marker for %s: %s", job_id, exc)
            continue
        if now - updated_at >= HIVE_LEASE_SECONDS:
            continue
        hive.heartbeat(
            urn=claimant,
            agent_token=claimant_token,
            required=True,
        )
        if not hive.patch_status(
            job_id,
            "running",
            result={"note": "active on node-relay worker"},
            claimed_by=claimant,
            agent_token=claimant_token,
        ):
            raise HiveClaimError(f"failed to renew active Hive job {job_id}")
        renewed += 1
    return renewed


def main() -> None:
    ap = argparse.ArgumentParser(description="node-relay enqueuer (home side)")
    ap.add_argument("--interval", type=float, default=15.0, help="poll seconds")
    ap.add_argument("--once", action="store_true", help="single drain then exit")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    host = _require_host()
    key = relay_crypto.load_key()
    hive = HiveClient(
        urn=f"urn:agent:system:{host}:node-relay-enqueuer",
        runtime="system",
        kind="system",
        host=host,
        capabilities=["*"],
    )
    relay = RelayClient()
    hive.register(metadata=gpu_metadata(relay, key))

    if args.once:
        renew_active_jobs(hive, relay, key)
        run_once(hive, relay, key)
        return

    attempt = 0
    while True:
        try:
            # heartbeat carries fresh remote GPU telemetry -> dashboard GPU panel
            hive.heartbeat(metadata=gpu_metadata(relay, key))
            renew_active_jobs(hive, relay, key)
            run_once(hive, relay, key)
            attempt = 0
            time.sleep(args.interval)
        except KeyboardInterrupt:
            log.info("enqueuer stopped")
            return
        except Exception:  # noqa: BLE001 — never die on transient errors
            attempt += 1
            log.exception("enqueuer loop error (attempt %d)", attempt)
            backoff_sleep(attempt)


if __name__ == "__main__":
    main()
