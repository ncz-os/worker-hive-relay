import base64
import sys

from node_relay import reconciler, relay_crypto
from node_relay.relay_client import RelayClient
from tests.node_relay.test_relay_flow import InMemoryBackend


def test_reconciler_registers_as_worker_runtime(monkeypatch):
    created = []

    class Hive:
        def __init__(self, *args, **kwargs):
            created.append((args, kwargs))

        def register(self):
            pass

    monkeypatch.setattr(reconciler, "HiveClient", Hive)
    monkeypatch.setattr(reconciler.relay_crypto, "load_key", lambda: b"key")
    monkeypatch.setattr(reconciler, "RelayClient", lambda: object())
    monkeypatch.setattr(reconciler, "PatchLander", lambda: object())
    monkeypatch.setattr(reconciler, "run_once", lambda *args: 0)
    monkeypatch.setattr(sys, "argv", ["mnemos-node-relay-reconciler", "--once"])

    reconciler.main()

    assert created == [
        (
            (),
            {
                "urn": "urn:agent:system:pythia:mnemos-node-relay-reconciler",
                "runtime": "system",
                "kind": "system",
                "host": "pythia",
                "capabilities": ["*"],
            },
        )
    ]


def test_reconciler_uses_claimant_token_without_leaking_it_to_hive_result():
    key = relay_crypto.load_key(base64.b64encode(b"7" * 32).decode())
    relay = RelayClient(backend=InMemoryBackend())
    relay.put_terminal(
        "job-1",
        relay_crypto.seal(
            {
                "status": "done",
                "claimant_urn": "urn:claimant",
                "_claimant_token": "agent-session-token",
                "suggestion": "finished",
            },
            key,
            aad=relay_crypto.aad_for("terminal", "job-1"),
        ),
    )
    calls = []

    class Hive:
        def patch_status(self, *args, **kwargs):
            calls.append((args, kwargs))
            return True

    assert reconciler._reconcile_one(Hive(), relay, key, "job-1", object()) is True
    assert calls[0][1]["agent_token"] == "agent-session-token"
    assert "_claimant_token" not in calls[0][1]["result"]
