import base64
import time

import pytest

from node_relay import enqueuer, relay_crypto
from node_relay.bridge_common import HiveClaimError
from node_relay.relay_client import RelayClient
from tests.node_relay.test_relay_flow import InMemoryBackend


def test_payload_includes_exact_refs_before_deduplicated_semantic_context(monkeypatch):
    exact = {
        "mem_handoff": {"id": "mem_handoff", "content": "exact handoff"},
        "mem_runbook": {"id": "mem_runbook", "content": "exact runbook"},
    }
    monkeypatch.setattr(enqueuer, "mnemos_get", lambda memory_id: exact[memory_id])
    monkeypatch.setattr(
        enqueuer,
        "mnemos_search",
        lambda query, limit: [
            {"id": "mem_handoff", "content": "duplicate search hit"},
            {"id": "mem_related", "content": "semantic addition"},
        ],
    )

    payload = enqueuer.build_payload(
        {
            "id": "job-1",
            "description": "do the work",
            "mnemos_refs": ["mem_handoff", "mem_runbook"],
        }
    )

    assert payload["context"] == [
        exact["mem_handoff"],
        exact["mem_runbook"],
        {"id": "mem_related", "content": "semantic addition"},
    ]


def test_payload_uses_real_hive_model_and_workspace_fields(monkeypatch):
    monkeypatch.setattr(enqueuer, "mnemos_search", lambda query, limit: [])

    claimed = enqueuer.build_payload(
        {
            "id": "job-1",
            "description": "edit it",
            "claimed_resources": {"model": "claimed-model"},
            "preferred_models": ["preferred-model"],
            "project": "mnemos",
        }
    )
    preferred = enqueuer.build_payload(
        {
            "id": "job-2",
            "description": "edit it",
            "claimed_resources": {"model": "unknown"},
            "preferred_models": ["preferred-model", "fallback-model"],
            "target_workspace": "riskyeats",
        }
    )

    assert claimed["model"] == "claimed-model"
    assert claimed["workspace"] == "mnemos"
    assert preferred["model"] == "preferred-model"
    assert preferred["workspace"] == "riskyeats"


def test_enqueue_persists_claimant_session_token_in_encrypted_routing(monkeypatch):
    key = relay_crypto.load_key(base64.b64encode(b"6" * 32).decode())
    relay = RelayClient(backend=InMemoryBackend())
    monkeypatch.setattr(enqueuer, "NODE_HOST", "node-x")
    monkeypatch.setattr(enqueuer, "mnemos_search", lambda query, limit: [])

    class Hive:
        urn = "urn:assigned-claimant"
        agent_token = "claimant-session-token"

        def __init__(self):
            self.jobs = [{"id": "job-1", "kind": "research:test", "description": "work"}]

        def claim_next(self):
            return self.jobs.pop() if self.jobs else None

        def patch_status(self, *args, **kwargs):
            return True

    assert enqueuer.run_once(Hive(), relay, key) == 1
    pending = relay_crypto.open_blob(
        relay.get_pending("job-1"),
        key,
        aad=relay_crypto.aad_for("pending", "job-1"),
    )
    claimant = relay_crypto.open_blob(
        relay.get_claimant("job-1"),
        key,
        aad=relay_crypto.aad_for("claimant", "job-1"),
    )
    assert pending["_claimant_token"] == "claimant-session-token"
    assert claimant == {
        "claimant_urn": "urn:assigned-claimant",
        "claimant_token": "claimant-session-token",
    }


def test_persisted_node_activity_renews_hive_after_enqueuer_restart():
    key = relay_crypto.load_key(base64.b64encode(b"2" * 32).decode())
    relay = RelayClient(backend=InMemoryBackend())
    relay.put_pending("job-1", b"sealed-pending")
    name = "active/job-1"
    relay.put_active(
        "job-1",
        relay_crypto.seal(
            {
                "job_id": "job-1",
                "claimant_urn": "urn:old-enqueuer-session",
                "owner": "remote-worker",
                "updated_at": time.time(),
            },
            key,
            aad=relay_crypto.aad_for("status", name),
        ),
    )
    calls = []

    class RestartedHive:
        urn = "urn:new-enqueuer-session"

        def heartbeat(self, *args, **kwargs):
            calls.append(("heartbeat", args, kwargs))
            return True

        def patch_status(self, *args, **kwargs):
            calls.append(("patch", args, kwargs))
            return True

    assert enqueuer.renew_active_jobs(RestartedHive(), relay, key) == 1
    assert calls == [
        (
            "heartbeat",
            (),
            {
                "urn": "urn:old-enqueuer-session",
                "agent_token": None,
                "required": True,
            },
        ),
        (
            "patch",
            ("job-1", "running"),
            {
                "result": {"note": "active on node-relay worker"},
                "claimed_by": "urn:old-enqueuer-session",
                "agent_token": None,
            },
        )
    ]


def test_stale_node_activity_does_not_renew_hive(monkeypatch):
    key = relay_crypto.load_key(base64.b64encode(b"3" * 32).decode())
    relay = RelayClient(backend=InMemoryBackend())
    relay.put_pending("job-1", b"sealed-pending")
    monkeypatch.setattr(enqueuer, "HIVE_LEASE_SECONDS", 10.0)
    relay.put_active(
        "job-1",
        relay_crypto.seal(
            {
                "job_id": "job-1",
                "claimant_urn": "urn:claimant",
                "owner": "dead-worker",
                "updated_at": time.time() - 11.0,
            },
            key,
            aad=relay_crypto.aad_for("status", "active/job-1"),
        ),
    )

    class Hive:
        def heartbeat(self, *args, **kwargs):
            raise AssertionError("stale activity must not heartbeat the old claimant")

        def patch_status(self, *args, **kwargs):
            raise AssertionError("stale activity must not extend the Hive lease")

    assert enqueuer.renew_active_jobs(Hive(), relay, key) == 0


def test_active_hive_renewal_failure_enters_backoff_path():
    key = relay_crypto.load_key(base64.b64encode(b"4" * 32).decode())
    relay = RelayClient(backend=InMemoryBackend())
    relay.put_pending("job-1", b"sealed-pending")
    relay.put_active(
        "job-1",
        relay_crypto.seal(
            {
                "job_id": "job-1",
                "claimant_urn": "urn:claimant",
                "owner": "remote-worker",
                "updated_at": time.time(),
            },
            key,
            aad=relay_crypto.aad_for("status", "active/job-1"),
        ),
    )

    class Hive:
        def heartbeat(self, *args, **kwargs):
            return True

        def patch_status(self, *args, **kwargs):
            return False

    with pytest.raises(HiveClaimError, match="job-1"):
        enqueuer.renew_active_jobs(Hive(), relay, key)


def test_old_claimant_heartbeat_failure_prevents_lease_patch():
    key = relay_crypto.load_key(base64.b64encode(b"5" * 32).decode())
    relay = RelayClient(backend=InMemoryBackend())
    relay.put_pending("job-1", b"sealed-pending")
    relay.put_active(
        "job-1",
        relay_crypto.seal(
            {
                "job_id": "job-1",
                "claimant_urn": "urn:old-claimant",
                "claimant_token": "old-agent-token",
                "owner": "remote-worker",
                "updated_at": time.time(),
            },
            key,
            aad=relay_crypto.aad_for("status", "active/job-1"),
        ),
    )

    class Hive:
        def heartbeat(self, *args, **kwargs):
            assert kwargs == {
                "urn": "urn:old-claimant",
                "agent_token": "old-agent-token",
                "required": True,
            }
            raise HiveClaimError("heartbeat rejected")

        def patch_status(self, *args, **kwargs):
            raise AssertionError("a dead claimant must not have its job lease renewed")

    with pytest.raises(HiveClaimError, match="heartbeat rejected"):
        enqueuer.renew_active_jobs(Hive(), relay, key)
