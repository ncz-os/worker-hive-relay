"""node-relay queue flow + idempotency tests against an in-memory backend.

These exercise the exactly-once / lease / create-only / purge guarantees of
:class:`RelayClient` without any GCS or S3 network — the in-memory backend
implements the same conditional-create + CAS primitives both real backends do.
"""

import base64
import json

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


def test_terminal_is_create_only(relay):
    a = relay_crypto.seal({"status": "done"}, KEY, aad=relay_crypto.aad_for("terminal", "u1"))
    b = relay_crypto.seal({"status": "failed"}, KEY, aad=relay_crypto.aad_for("terminal", "u1"))
    assert relay.put_terminal("u1", a) is True
    assert relay.put_terminal("u1", b) is False  # first writer wins; no double terminal
    got = relay_crypto.open_blob(relay.get_terminal("u1"), KEY, aad=relay_crypto.aad_for("terminal", "u1"))
    assert got["status"] == "done"


def test_purge_is_idempotent(relay):
    relay.put_pending("u1", b"x")
    relay.claim("u1", "w")
    relay.put_terminal("u1", relay_crypto.seal({"status": "done"}, KEY, aad=relay_crypto.aad_for("terminal", "u1")))
    relay.purge("u1")
    assert relay.list_pending() == []
    assert relay.list_terminal() == []
    relay.purge("u1")  # second purge must not raise
    relay.purge("never-existed")


def test_status_overwrite_latest_wins(relay):
    relay.put_status("node-x-gpu", b"v1")
    relay.put_status("node-x-gpu", b"v2")
    assert relay.get_status("node-x-gpu") == b"v2"
    assert relay.get_status("absent") is None


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
