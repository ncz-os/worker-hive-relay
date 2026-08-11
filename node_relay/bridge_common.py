"""Shared HTTP clients + helpers for the relay's home-side processes.

Talks to the GRAEAE Hive Mind bus (:5005) and MNEMOS (:5002) over HTTP so the
relay stays decoupled from the in-process repository layer. The remote node
side does NOT import this module (it never reaches the home fleet).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any
from urllib.parse import quote

import requests

log = logging.getLogger("node_relay")

HIVE_BASE = os.environ.get("HIVE_BASE", "http://192.168.207.67:5005")
# C1: bus transport auth bearer token (env-sourced, never baked in). Applied as
# a default header on the dedicated hive session below; mnemos_search uses its
# own MNEMOS_TOKEN and is unaffected.
HIVE_BUS_TOKEN = os.environ.get("HIVE_BUS_TOKEN", "")
MNEMOS_BASE = os.environ.get("MNEMOS_BASE", "http://192.168.207.67:5002")
MNEMOS_TOKEN = os.environ.get("MNEMOS_TOKEN", "")
_TIMEOUT = float(os.environ.get("RELAY_HTTP_TIMEOUT", "30"))


class HiveClaimError(RuntimeError):
    """The hive queue could not be queried reliably."""


class HiveRegistrationError(RuntimeError):
    """The relay agent could not be registered with Hive."""


class MnemosSearchError(RuntimeError):
    """MNEMOS context could not be retrieved reliably."""


class HiveClient:
    """Minimal client for the hive endpoints the bridge needs.

    Node routing is by HOST, not kind: dequeue eligibility filters a job's
    ``eligible_hosts`` against the agent host parsed from its URN
    (``urn:agent:<kind>:<host>:<sid>``). So the enqueuer registers AS the remote
    node — ``urn:agent:system:<node-host>:…`` — and drains jobs submitted with
    ``eligible_hosts=["<node-host>"]``. ``capabilities=["*"]`` bypasses
    capability/workspace claim gates. The reconciler only PATCHes.
    """

    def __init__(
        self,
        urn: str,
        *,
        runtime: str,
        kind: str,
        host: str,
        capabilities: list[str] | None = None,
        provider: str = "unknown",
        model: str = "unknown",
        base: str = HIVE_BASE,
    ):
        self.base = base.rstrip("/")
        self.urn = urn
        self.runtime = runtime
        self.kind = kind
        self.host = host
        self.capabilities = capabilities
        self.provider = provider
        self.model = model
        self.agent_token: str | None = None
        self._session = requests.Session()
        if HIVE_BUS_TOKEN:
            self._session.headers["Authorization"] = f"Bearer {HIVE_BUS_TOKEN}"

    def register(self, metadata: dict[str, Any] | None = None) -> None:
        """Register and ADOPT the server-assigned URN. The hive replaces the
        session segment with its own uuid; subsequent claim/patch must use the
        returned URN or the agent reads as 'not registered'. ``metadata`` is
        merged into the role tag (e.g. specs.gpus so /v1/hosts surfaces it)."""
        meta = {"role": "node-relay-bridge", **(metadata or {})}
        try:
            resp = self._session.post(
                f"{self.base}/v1/agents/register",
                json={
                    "urn": self.urn,
                    "runtime": self.runtime,
                    "kind": self.kind,
                    "host": self.host,
                    "provider": self.provider,
                    "model": self.model,
                    "capabilities": self.capabilities,
                    "autonomy_level": "autonomous",
                    "metadata": meta,
                },
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise ValueError("register response was not an object")
            assigned = data.get("urn")
            if not isinstance(assigned, str) or not assigned:
                raise ValueError("register response did not include an assigned urn")
            self.urn = assigned
            agent_token = data.get("agent_token")
            if agent_token is not None and (not isinstance(agent_token, str) or not agent_token):
                raise ValueError("register response included an invalid agent token")
            self.agent_token = agent_token
        except (requests.RequestException, ValueError) as exc:
            raise HiveRegistrationError(f"hive register failed: {exc}") from exc

    @staticmethod
    def _agent_headers(agent_token: str | None) -> dict[str, str] | None:
        return {"Authorization": f"Bearer {agent_token}"} if agent_token else None

    def heartbeat(
        self,
        status: str = "online",
        metadata: dict[str, Any] | None = None,
        *,
        urn: str | None = None,
        agent_token: str | None = None,
        required: bool = False,
    ) -> bool:
        """Keep the agent active. The hive reaps agents to 'stale' after ~90s
        without a heartbeat, after which dequeue stops offering work — so the
        loops must call this every iteration (interval < the stale window).
        ``metadata`` (e.g. {"load": {"gpus_runtime": [...]}}) is merged
        server-side so /v1/hosts shows fresh GPU telemetry."""
        target_urn = urn or self.urn
        token = agent_token if urn is not None else (agent_token or self.agent_token)
        body: dict[str, Any] = {"urn": target_urn, "status": status}
        if metadata:
            body["metadata"] = metadata
        try:
            self._session.post(
                f"{self.base}/v1/agents/heartbeat",
                json=body,
                headers=self._agent_headers(token),
                timeout=_TIMEOUT,
            ).raise_for_status()
            return True
        except requests.RequestException as exc:
            if required:
                raise HiveClaimError(f"hive heartbeat failed for {target_urn}: {exc}") from exc
            log.warning("heartbeat failed (non-fatal): %s", exc)
            return False

    def claim_next(self) -> dict[str, Any] | None:
        """Atomically dequeue+claim the next job this agent is eligible for, or
        None if the queue is dry. Eligibility = job.eligible_hosts covers this
        agent's host (parsed from the URN at registration).

        Only 204 means there is no claim this round. State conflicts, transport,
        authentication, server, and malformed-response failures raise
        :class:`HiveClaimError` so the service loop enters backoff."""
        try:
            resp = self._session.post(
                f"{self.base}/v1/jobs/next",
                params={"agent_urn": self.urn},
                headers=self._agent_headers(self.agent_token),
                timeout=_TIMEOUT,
            )
        except requests.RequestException as exc:
            raise HiveClaimError(f"claim_next transport failure: {exc}") from exc
        if resp.status_code == 204:
            return None
        if resp.status_code >= 400:
            raise HiveClaimError(
                f"claim_next HTTP {resp.status_code}: {resp.text[:200]}"
            )
        try:
            data = resp.json()
        except ValueError as exc:
            raise HiveClaimError("claim_next returned malformed JSON") from exc
        if not isinstance(data, dict) or not data:
            raise HiveClaimError("claim_next returned an invalid job payload")
        return data

    def patch_status(
        self,
        job_id: str,
        status: str,
        *,
        result: dict[str, Any] | None = None,
        claimed_by: str | None = None,
        agent_token: str | None = None,
    ) -> bool:
        """PATCH a job to ``status`` per the JobUpdate contract. Non-status data
        (commit_sha, branch, metrics, error) goes inside ``result``.

        ``claimed_by`` MUST equal the job's current claimant or the hive returns
        403. The reconciler (a different agent than the enqueuer that claimed the
        job) passes the job's actual claimant; the enqueuer omits it and defaults
        to its own URN (it is the claimant)."""
        code = self.patch_status_code(
            job_id,
            status,
            result=result,
            claimed_by=claimed_by,
            agent_token=agent_token,
        )
        return code is not None and 200 <= code < 300

    def patch_status_code(
        self,
        job_id: str,
        status: str,
        *,
        result: dict[str, Any] | None = None,
        claimed_by: str | None = None,
        agent_token: str | None = None,
    ) -> int | None:
        """Like :meth:`patch_status` but returns the HTTP status code (or None
        on a network-level failure) so callers can distinguish validation
        rejections (4xx — e.g. a bus that doesn't know a status enum) from
        transient transport errors that should be retried, not worked around."""
        body: dict[str, Any] = {"status": status, "claimed_by": claimed_by or self.urn}
        if result is not None:
            body["result"] = result
        try:
            token = agent_token or (self.agent_token if claimed_by in (None, self.urn) else None)
            resp = self._session.patch(
                f"{self.base}/v1/jobs/{job_id}",
                json=body,
                headers=self._agent_headers(token),
                timeout=_TIMEOUT,
            )
            if resp.status_code >= 400:
                log.error(
                    "patch_status %s -> %s rejected %s: %s",
                    job_id, status, resp.status_code, resp.text[:200],
                )
            return resp.status_code
        except requests.RequestException as exc:
            log.error("patch_status %s -> %s failed: %s", job_id, status, exc)
            return None


def mnemos_search(query: str, limit: int = 6) -> list[dict[str, Any]]:
    """Retrieve relevant MNEMOS context for context-prepackaging.

    Returns a list of ``{"id", "content"}`` dicts. A legitimate empty result
    returns ``[]``; transport, authentication, and malformed-response failures
    raise :class:`MnemosSearchError` so they cannot masquerade as empty context.
    Endpoint/shape per the MNEMOS HTTP API (Bearer auth).
    """
    if not query.strip():
        return []
    headers = {"Authorization": f"Bearer {MNEMOS_TOKEN}"} if MNEMOS_TOKEN else {}
    try:
        resp = requests.post(
            f"{MNEMOS_BASE}/v1/memories/search",
            json={"query": query, "limit": limit, "semantic": True},
            headers=headers,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        raise MnemosSearchError(f"mnemos_search failed: {exc}") from exc
    if isinstance(data, dict):
        rows = data.get("memories")
    elif isinstance(data, list):
        rows = data
    else:
        rows = None
    if not isinstance(rows, list):
        raise MnemosSearchError("mnemos_search returned an invalid response payload")
    out: list[dict[str, Any]] = []
    for r in rows or []:
        if isinstance(r, dict) and r.get("content"):
            out.append({"id": r.get("id"), "content": r["content"]})
    return out


def mnemos_get(memory_id: str) -> dict[str, Any]:
    """Retrieve one explicitly referenced MNEMOS memory by id.

    Exact references are mandatory job context, so missing, inaccessible, or
    malformed memories raise instead of silently degrading to semantic search.
    """
    if not isinstance(memory_id, str) or not memory_id:
        raise MnemosSearchError("mnemos_get requires a non-empty memory id")
    headers = {"Authorization": f"Bearer {MNEMOS_TOKEN}"} if MNEMOS_TOKEN else {}
    try:
        resp = requests.get(
            f"{MNEMOS_BASE}/v1/memories/{quote(memory_id, safe='')}",
            headers=headers,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        raise MnemosSearchError(f"mnemos_get {memory_id!r} failed: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("content"), str):
        raise MnemosSearchError(f"mnemos_get {memory_id!r} returned an invalid response payload")
    return {"id": data.get("id") or memory_id, "content": data["content"]}


def backoff_sleep(attempt: int, base: float = 2.0, cap: float = 60.0) -> None:
    """Deterministic exponential backoff (no jitter — single bridge process)."""
    time.sleep(min(cap, base * (2 ** max(0, attempt - 1))))
