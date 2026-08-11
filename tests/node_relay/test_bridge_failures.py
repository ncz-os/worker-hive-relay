import base64

import pytest
import requests

from node_relay import relay_crypto
from node_relay.bridge_common import (
    HiveClaimError,
    HiveClient,
    HiveRegistrationError,
    MnemosSearchError,
    mnemos_get,
    mnemos_search,
)
from node_relay.node_poller import run_once
from node_relay.relay_client import RelayClient
from tests.node_relay.test_relay_flow import InMemoryBackend


class _Response:
    def __init__(self, status_code, *, text="", payload=None):
        self.status_code = status_code
        self.text = text
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class _Session:
    def __init__(self, response):
        self.response = response

    def post(self, *args, **kwargs):
        return self.response


def _hive_with_response(response):
    hive = HiveClient("urn:test", runtime="test", kind="test", host="host")
    hive._session = _Session(response)
    return hive


def test_hive_empty_queue_remains_none():
    assert _hive_with_response(_Response(204)).claim_next() is None


def test_hive_state_conflict_does_not_masquerade_as_empty_queue():
    with pytest.raises(HiveClaimError, match="409"):
        _hive_with_response(_Response(409, text="agent is stale")).claim_next()


@pytest.mark.parametrize("status", [401, 403, 500, 503])
def test_hive_failures_do_not_masquerade_as_empty_queue(status):
    with pytest.raises(HiveClaimError, match=str(status)):
        _hive_with_response(_Response(status, text="failure")).claim_next()


def test_hive_success_with_invalid_payload_is_not_an_empty_queue():
    with pytest.raises(HiveClaimError, match="invalid job payload"):
        _hive_with_response(_Response(200, payload={})).claim_next()


def test_mnemos_failure_does_not_masquerade_as_empty_context(monkeypatch):
    def fail(*args, **kwargs):
        raise requests.Timeout("timed out")

    monkeypatch.setattr(requests, "post", fail)
    with pytest.raises(MnemosSearchError, match="timed out"):
        mnemos_search("context")


def test_mnemos_malformed_payload_is_not_empty_context(monkeypatch):
    response = _Response(200, payload={"unexpected": []})
    response.raise_for_status = lambda: None
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: response)
    with pytest.raises(MnemosSearchError, match="invalid response payload"):
        mnemos_search("context")


def test_registration_failure_is_fatal():
    hive = _hive_with_response(_Response(503, text="unavailable"))
    with pytest.raises(HiveRegistrationError, match="503"):
        hive.register()


def test_registration_requires_and_adopts_server_urn():
    missing = _hive_with_response(_Response(200, payload={}))
    with pytest.raises(HiveRegistrationError, match="assigned urn"):
        missing.register()

    hive = _hive_with_response(_Response(200, payload={"urn": "urn:assigned"}))
    hive.register()
    assert hive.urn == "urn:assigned"


def test_mnemos_get_resolves_exact_reference(monkeypatch):
    response = _Response(200, payload={"id": "mem_exact", "content": "required decision"})
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)

    assert mnemos_get("mem_exact") == {"id": "mem_exact", "content": "required decision"}


def test_corrupt_pending_uses_authenticated_claimant_envelope():
    relay = RelayClient(backend=InMemoryBackend())
    key = relay_crypto.load_key(base64.b64encode(b"1" * 32).decode())
    relay.put_claimant(
        "u1",
        relay_crypto.seal(
            {"claimant_urn": "urn:original-claimant"},
            key,
            aad=relay_crypto.aad_for("claimant", "u1"),
        ),
    )
    relay.put_pending("u1", b"corrupt")

    class NeverExecutor:
        def execute(self, _job):
            raise AssertionError("corrupt jobs must not execute")

    assert run_once(relay, key, NeverExecutor(), owner="worker-a") == 1
    terminal = relay_crypto.open_blob(
        relay.get_terminal("u1"), key, aad=relay_crypto.aad_for("terminal", "u1")
    )
    assert terminal["status"] == "failed"
    assert terminal["claimant_urn"] == "urn:original-claimant"
