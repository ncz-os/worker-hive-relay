"""node-relay queue flow + idempotency tests against an in-memory backend.

These exercise the exactly-once / lease / create-only / purge guarantees of
:class:`RelayClient` without any GCS or S3 network — the in-memory backend
implements the same conditional-create + CAS primitives both real backends do.
"""

import base64
import json
import time

import pytest

from node_relay import relay_crypto
from node_relay.relay_client import RelayClient, RelayObjectNotFound, _Backend


class InMemoryBackend(_Backend):
    """Versioned object store with the same conditional semantics as GCS/S3."""

    def __init__(self):
        self.objs: dict[str, tuple[bytes, int]] = {}  # key -> (body, version)
        self._seq = 0

    def _next(self) -> int:
        self._seq += 1
        return self._seq

    def create_only(self, key, body):
        if key in self.objs:
            return False
        self.objs[key] = (body, self._next())
        return True

    def cas_write(self, key, body, version):
        cur = self.objs.get(key)
        if cur is None or str(cur[1]) != str(version):
            return False
        self.objs[key] = (body, self._next())
        return True

    def read(self, key):
        if key not in self.objs:
            raise RelayObjectNotFound(key)
        return self.objs[key][0]

    def read_opt(self, key):
        return self.objs[key][0] if key in self.objs else None

    def read_with_version(self, key):
        if key not in self.objs:
            return None
        body, ver = self.objs[key]
        return body, str(ver)

    def put(self, key, body):
        self.objs[key] = (body, self._next())

    def list_keys(self, prefix):
        return [k for k in self.objs if k.startswith(prefix)]

    def delete(self, key):
        self.objs.pop(key, None)  # idempotent


@pytest.fixture
def relay():
    return RelayClient(backend=InMemoryBackend())


KEY = relay_crypto.load_key(base64.b64encode(b"0" * 32).decode())


def test_pending_put_list_get_roundtrip(relay):
    sealed = relay_crypto.seal({"job_id": "u1", "prompt": "hi"}, KEY, aad=relay_crypto.aad_for("pending", "u1"))
    relay.put_pending("u1", sealed)
    assert relay.list_pending() == ["u1"]
    got = relay_crypto.open_blob(relay.get_pending("u1"), KEY, aad=relay_crypto.aad_for("pending", "u1"))
    assert got["prompt"] == "hi"


def test_bounded_pending_scan_rotates_without_starving_later_keys(relay):
    for uuid in ("u1", "u2", "u3"):
        relay.put_pending(uuid, b"sealed")

    assert list(relay.iter_pending(max_items=2)) == ["u1", "u2"]
    assert list(relay.iter_pending(max_items=2)) == ["u3", "u1"]
    assert list(relay.iter_pending(max_items=2)) == ["u2", "u3"]


def test_claim_is_exactly_once(relay):
    assert relay.claim("u1", "worker-a") is True
    assert relay.claim("u1", "worker-b") is False  # already held, lease live


def test_expired_lease_taken_over(relay):
    assert relay.claim("u1", "worker-a", now=1000.0) is True
    # within lease -> denied
    assert relay.claim("u1", "worker-b", now=1000.0 + 10, lease_seconds=100) is False
    # past lease -> taken over
    assert relay.claim("u1", "worker-b", now=1000.0 + 200, lease_seconds=100) is True
    body, _ = relay._backend.objs["claimed/u1"]
    assert json.loads(body)["owner"] == "worker-b"


def test_claim_renewal_is_owner_and_version_checked(relay):
    assert relay.claim("u1", "worker-a", now=1000.0) is True
    assert relay.renew_claim("u1", "worker-b", now=1010.0) is False
    assert relay.renew_claim("u1", "worker-a", now=1010.0) is True
    assert relay.owns_claim("u1", "worker-a", now=1050.0, lease_seconds=100) is True
    assert relay.claim("u1", "worker-b", now=1050.0, lease_seconds=100) is False


def test_terminal_is_create_only(relay):
    a = relay_crypto.seal({"status": "done"}, KEY, aad=relay_crypto.aad_for("terminal", "u1"))
    b = relay_crypto.seal({"status": "failed"}, KEY, aad=relay_crypto.aad_for("terminal", "u1"))
    assert relay.put_terminal("u1", a) is True
    assert relay.put_terminal("u1", b) is False  # first writer wins; no double terminal
    got = relay_crypto.open_blob(relay.get_terminal("u1"), KEY, aad=relay_crypto.aad_for("terminal", "u1"))
    assert got["status"] == "done"


def test_purge_is_idempotent(relay):
    relay.put_pending("u1", b"x")
    relay.put_claimant("u1", b"routing")
    relay.claim("u1", "w")
    relay.put_terminal("u1", relay_crypto.seal({"status": "done"}, KEY, aad=relay_crypto.aad_for("terminal", "u1")))
    relay.put_active("u1", b"active")
    relay.purge("u1")
    assert relay.list_pending() == []
    assert relay.list_terminal() == []
    assert relay._backend.read_opt("claimant/u1.json.enc") is None
    assert relay.get_active("u1") is None
    relay.purge("u1")  # second purge must not raise
    relay.purge("never-existed")


def test_status_overwrite_latest_wins(relay):
    relay.put_status("node-x-gpu", b"v1")
    relay.put_status("node-x-gpu", b"v2")
    assert relay.get_status("node-x-gpu") == b"v2"
    assert relay.get_status("absent") is None


def test_long_execution_renews_claim_and_prevents_takeover(relay, monkeypatch):
    from node_relay.node_poller import run_once

    monkeypatch.setenv("NODE_RELAY_LEASE_SECONDS", "0.12")
    job = {"job_id": "u1", "prompt": "work", "kind": "research:test", "claimant_urn": "urn:hive"}
    relay.put_pending(
        "u1", relay_crypto.seal(job, KEY, aad=relay_crypto.aad_for("pending", "u1"))
    )

    class SlowExecutor:
        def execute(self, _job):
            time.sleep(0.2)
            assert relay.claim("u1", "worker-b", lease_seconds=0.12) is False
            return {"status": "done"}

    assert run_once(relay, KEY, SlowExecutor(), owner="worker-a") == 1
    assert relay.list_terminal() == ["u1"]
    activity = relay_crypto.open_blob(
        relay.get_active("u1"), KEY, aad=relay_crypto.aad_for("status", "active/u1")
    )
    assert activity["job_id"] == "u1"
    assert activity["claimant_urn"] == "urn:hive"
    assert activity["owner"] == "worker-a"


def test_lost_owner_cannot_write_terminal(relay, monkeypatch):
    from node_relay.node_poller import run_once

    monkeypatch.setenv("NODE_RELAY_LEASE_SECONDS", "10")
    job = {"job_id": "u1", "prompt": "work", "kind": "research:test", "claimant_urn": "urn:hive"}
    relay.put_pending(
        "u1", relay_crypto.seal(job, KEY, aad=relay_crypto.aad_for("pending", "u1"))
    )

    class OwnershipStealingExecutor:
        def execute(self, _job):
            body, version = relay._backend.read_with_version("claimed/u1")
            assert json.loads(body)["owner"] == "worker-a"
            replacement = json.dumps({"owner": "worker-b", "claimed_at": time.time()}).encode()
            assert relay._backend.cas_write("claimed/u1", replacement, version)
            return {"status": "done"}

    assert run_once(relay, KEY, OwnershipStealingExecutor(), owner="worker-a") == 1
    assert relay.list_terminal() == []


def test_claimant_token_is_routed_but_not_exposed_to_executor(relay):
    from node_relay.node_poller import run_once

    job = {
        "job_id": "u1",
        "prompt": "work",
        "kind": "research:test",
        "claimant_urn": "urn:hive",
        "_claimant_token": "agent-session-token",
    }
    relay.put_pending(
        "u1", relay_crypto.seal(job, KEY, aad=relay_crypto.aad_for("pending", "u1"))
    )

    class Executor:
        def execute(self, opened_job):
            assert "_claimant_token" not in opened_job
            return {"status": "done"}

    assert run_once(relay, KEY, Executor(), owner="worker-a") == 1
    activity = relay_crypto.open_blob(
        relay.get_active("u1"), KEY, aad=relay_crypto.aad_for("status", "active/u1")
    )
    terminal = relay_crypto.open_blob(
        relay.get_terminal("u1"), KEY, aad=relay_crypto.aad_for("terminal", "u1")
    )
    assert activity["claimant_token"] == "agent-session-token"
    assert terminal["_claimant_token"] == "agent-session-token"


def test_activity_write_failure_records_terminal_without_executing():
    from node_relay.node_poller import run_once

    class ActivityDeniedBackend(InMemoryBackend):
        def put(self, key, body):
            if key.startswith("status/active/"):
                raise PermissionError("active prefix denied")
            super().put(key, body)

    denied_relay = RelayClient(backend=ActivityDeniedBackend())
    job = {
        "job_id": "u1",
        "prompt": "work",
        "kind": "research:test",
        "claimant_urn": "urn:hive",
    }
    denied_relay.put_pending(
        "u1", relay_crypto.seal(job, KEY, aad=relay_crypto.aad_for("pending", "u1"))
    )

    class NeverExecutor:
        def execute(self, _job):
            raise AssertionError("job without activity must not execute")

    assert run_once(denied_relay, KEY, NeverExecutor(), owner="worker-a") == 1
    terminal = relay_crypto.open_blob(
        denied_relay.get_terminal("u1"),
        KEY,
        aad=relay_crypto.aad_for("terminal", "u1"),
    )
    assert terminal["status"] == "failed"
    assert terminal["claimant_urn"] == "urn:hive"


def test_transient_activity_write_failure_is_retried_before_execution():
    from node_relay.node_poller import run_once

    class FailFirstActivityBackend(InMemoryBackend):
        def __init__(self):
            super().__init__()
            self.activity_attempts = 0

        def put(self, key, body):
            if key.startswith("status/active/"):
                self.activity_attempts += 1
                if self.activity_attempts == 1:
                    raise ConnectionError("transient activity failure")
            super().put(key, body)

    backend = FailFirstActivityBackend()
    retry_relay = RelayClient(backend=backend)
    job = {
        "job_id": "u1",
        "prompt": "work",
        "kind": "research:test",
        "claimant_urn": "urn:hive",
    }
    retry_relay.put_pending(
        "u1", relay_crypto.seal(job, KEY, aad=relay_crypto.aad_for("pending", "u1"))
    )

    class Executor:
        def execute(self, _job):
            return {"status": "done"}

    assert run_once(retry_relay, KEY, Executor(), owner="worker-a") == 1
    terminal = relay_crypto.open_blob(
        retry_relay.get_terminal("u1"),
        KEY,
        aad=relay_crypto.aad_for("terminal", "u1"),
    )
    assert terminal["status"] == "done"
    assert backend.activity_attempts >= 2


def test_activity_write_failure_does_not_lose_successfully_renewed_claim(relay):
    from node_relay.node_poller import _ClaimLease

    assert relay.claim("u1", "worker-a") is True
    lease = _ClaimLease(relay, "u1", "worker-a", 60, KEY)
    lease.claimant = "urn:hive"
    relay.put_active = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        PermissionError("active prefix denied")
    )

    assert lease._renew() is True
    assert lease.is_owned() is True


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
