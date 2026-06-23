"""Cloud-object transport for the node-relay queue (GCS or S3).

The relay is a tiny exactly-once job queue layered on a public-cloud object
store. It works against any node that has internet egress but no LAN/VPN path
to the home fleet — the bucket is the only shared surface. Two backends are
supported behind one interface, selected at runtime:

* **GCS** — conditional create via ``ifGenerationMatch=0`` and CAS via the
  object generation.
* **S3** (AWS, or any S3-compatible endpoint) — conditional create via
  ``If-None-Match: *`` and CAS via ``If-Match: <etag>``.

Bucket layout (every ``*.json.enc`` value is a sealed blob from
:mod:`relay_crypto`; ``claimed/<uuid>`` holds a small JSON lease marker)::

    pending/<uuid>.json.enc     enqueuer (home) writes; remote node consumes
    claimed/<uuid>              node conditional-creates = lease lock (exactly-once)
    terminal/<uuid>.json.enc    node writes done OR failed (status in payload);
                                reconciler (home) consumes

The atomic primitive is :meth:`RelayClient.claim`, a conditional create
("create only if absent"). Two pollers racing the same job → exactly one wins;
the loser backs off. No external lock service is involved.

Backend selection (in order):
  1. ``$NODE_RELAY_BACKEND`` = ``gcs`` | ``s3``
  2. inferred from the bucket URL scheme (``gs://…`` → gcs, ``s3://…`` → s3)
  3. default ``gcs``
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass

_PENDING = "pending/"
_CLAIMED = "claimed/"
# Single terminal prefix (not split done/failed): one create-only object per job
# is the exactly-once gate, so two workers can never record conflicting terminal
# states for the same uuid. The done-vs-failed distinction lives in the sealed
# payload's "status" field.
_TERMINAL = "terminal/"
# Out-of-band status objects (e.g. node telemetry). Overwrite-allowed (latest
# wins), unlike the create-only terminal/claimed objects.
_STATUS = "status/"
_SUFFIX = ".json.enc"

# A claim older than this (seconds) is considered abandoned (worker died
# mid-job) and may be taken over. Must exceed the longest expected job runtime.
DEFAULT_LEASE_SECONDS = 7200.0


def _env(*names: str, default: str | None = None) -> str | None:
    """First set env var among ``names`` (current name preferred, legacy last)."""
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return default


class RelayObjectNotFound(Exception):
    """A requested object does not exist in the bucket."""


# --------------------------------------------------------------------------
# Backend interface + implementations
# --------------------------------------------------------------------------
class _Backend:
    """Object-store primitives the relay queue is built from. Keys are full
    object names (prefix included). All deletes are idempotent."""

    def create_only(self, key: str, body: bytes) -> bool:  # pragma: no cover - iface
        raise NotImplementedError

    def cas_write(self, key: str, body: bytes, version: str) -> bool:  # pragma: no cover
        raise NotImplementedError

    def read(self, key: str) -> bytes:  # pragma: no cover - iface
        raise NotImplementedError

    def read_opt(self, key: str) -> bytes | None:  # pragma: no cover - iface
        raise NotImplementedError

    def read_with_version(self, key: str) -> tuple[bytes, str] | None:  # pragma: no cover
        raise NotImplementedError

    def put(self, key: str, body: bytes) -> None:  # pragma: no cover - iface
        raise NotImplementedError

    def list_keys(self, prefix: str) -> list[str]:  # pragma: no cover - iface
        raise NotImplementedError

    def delete(self, key: str) -> None:  # pragma: no cover - iface
        raise NotImplementedError


class GcsBackend(_Backend):
    """Google Cloud Storage backend (conditional create via ifGenerationMatch)."""

    def __init__(self, bucket: str, sa_key_path: str | None) -> None:
        # Lazy import so the module stays importable (tests, CI, type checks)
        # without google-cloud-storage installed.
        from google.api_core.exceptions import NotFound, PreconditionFailed
        from google.cloud import storage

        self._NotFound = NotFound
        self._PreconditionFailed = PreconditionFailed
        if sa_key_path:
            self._client = storage.Client.from_service_account_json(sa_key_path)
        else:
            # Application Default Credentials (workload identity / gcloud auth).
            self._client = storage.Client()
        self._bucket = self._client.bucket(bucket)

    def create_only(self, key: str, body: bytes) -> bool:
        try:
            self._bucket.blob(key).upload_from_string(
                body, if_generation_match=0, content_type="application/octet-stream"
            )
            return True
        except self._PreconditionFailed:
            return False

    def cas_write(self, key: str, body: bytes, version: str) -> bool:
        try:
            self._bucket.blob(key).upload_from_string(
                body, if_generation_match=int(version), content_type="application/octet-stream"
            )
            return True
        except self._PreconditionFailed:
            return False

    def read(self, key: str) -> bytes:
        try:
            return self._bucket.blob(key).download_as_bytes()
        except self._NotFound as exc:
            raise RelayObjectNotFound(key) from exc

    def read_opt(self, key: str) -> bytes | None:
        try:
            return self.read(key)
        except RelayObjectNotFound:
            return None

    def read_with_version(self, key: str) -> tuple[bytes, str] | None:
        blob = self._bucket.get_blob(key)  # populates generation
        if blob is None or blob.generation is None:
            return None
        return blob.download_as_bytes(), str(blob.generation)

    def put(self, key: str, body: bytes) -> None:
        self._bucket.blob(key).upload_from_string(body, content_type="application/octet-stream")

    def list_keys(self, prefix: str) -> list[str]:
        return [b.name for b in self._client.list_blobs(self._bucket, prefix=prefix)]

    def delete(self, key: str) -> None:
        try:
            self._bucket.blob(key).delete()
        except self._NotFound:
            pass


class S3Backend(_Backend):
    """S3 backend (AWS or S3-compatible). Conditional create via If-None-Match,
    CAS via If-Match — both GA on AWS S3 PutObject. Credentials come from the
    standard boto3 chain (AWS_* env, shared config, instance role)."""

    def __init__(self, bucket: str, *, region: str | None, endpoint_url: str | None) -> None:
        import boto3  # lazy
        from botocore.exceptions import ClientError

        self._ClientError = ClientError
        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            region_name=region or None,
            endpoint_url=endpoint_url or None,
        )

    @staticmethod
    def _is_precondition(exc) -> bool:
        meta = getattr(exc, "response", {}) or {}
        code = (meta.get("Error") or {}).get("Code")
        status = (meta.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        return code in ("PreconditionFailed", "412") or status == 412

    @staticmethod
    def _is_missing(exc) -> bool:
        meta = getattr(exc, "response", {}) or {}
        code = (meta.get("Error") or {}).get("Code")
        status = (meta.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        return code in ("NoSuchKey", "404", "NotFound") or status == 404

    def create_only(self, key: str, body: bytes) -> bool:
        try:
            self._client.put_object(Bucket=self._bucket, Key=key, Body=body, IfNoneMatch="*")
            return True
        except self._ClientError as exc:
            if self._is_precondition(exc):
                return False
            raise

    def cas_write(self, key: str, body: bytes, version: str) -> bool:
        try:
            self._client.put_object(Bucket=self._bucket, Key=key, Body=body, IfMatch=version)
            return True
        except self._ClientError as exc:
            if self._is_precondition(exc):
                return False
            raise

    def read(self, key: str) -> bytes:
        try:
            return self._client.get_object(Bucket=self._bucket, Key=key)["Body"].read()
        except self._ClientError as exc:
            if self._is_missing(exc):
                raise RelayObjectNotFound(key) from exc
            raise

    def read_opt(self, key: str) -> bytes | None:
        try:
            return self.read(key)
        except RelayObjectNotFound:
            return None

    def read_with_version(self, key: str) -> tuple[bytes, str] | None:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=key)
        except self._ClientError as exc:
            if self._is_missing(exc):
                return None
            raise
        return resp["Body"].read(), resp["ETag"]

    def put(self, key: str, body: bytes) -> None:
        self._client.put_object(Bucket=self._bucket, Key=key, Body=body)

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token: str | None = None
        while True:
            kw = {"Bucket": self._bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = self._client.list_objects_v2(**kw)
            keys.extend(obj["Key"] for obj in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return keys

    def delete(self, key: str) -> None:
        # S3 delete of a missing key is a no-op (204), so this is idempotent.
        self._client.delete_object(Bucket=self._bucket, Key=key)


@dataclass(frozen=True)
class RelayConfig:
    bucket: str
    backend: str  # "gcs" | "s3"
    gcs_sa_key_path: str | None = None
    s3_region: str | None = None
    s3_endpoint_url: str | None = None

    @staticmethod
    def _split_scheme(raw: str) -> tuple[str | None, str]:
        for scheme, name in (("gs://", "gcs"), ("s3://", "s3")):
            if raw.startswith(scheme):
                return name, raw[len(scheme):]
        return None, raw

    @classmethod
    def from_env(cls) -> "RelayConfig":
        raw_bucket = _env("NODE_RELAY_BUCKET", "SPARK_HIVE_RELAY_BUCKET")
        if not raw_bucket:
            raise RuntimeError("set $NODE_RELAY_BUCKET (bare name, gs://name, or s3://name)")
        scheme_backend, bucket = cls._split_scheme(raw_bucket)
        backend = (_env("NODE_RELAY_BACKEND") or scheme_backend or "gcs").lower()
        if backend not in ("gcs", "s3"):
            raise RuntimeError(f"unknown $NODE_RELAY_BACKEND {backend!r} (want gcs|s3)")
        return cls(
            bucket=bucket,
            backend=backend,
            gcs_sa_key_path=_env("NODE_RELAY_GCS_SA", "SPARK_HIVE_RELAY_GCS_SA"),
            s3_region=_env("NODE_RELAY_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"),
            s3_endpoint_url=_env("NODE_RELAY_S3_ENDPOINT"),
        )

    def make_backend(self) -> _Backend:
        if self.backend == "s3":
            return S3Backend(self.bucket, region=self.s3_region, endpoint_url=self.s3_endpoint_url)
        return GcsBackend(self.bucket, self.gcs_sa_key_path)


class RelayClient:
    """Backend-agnostic relay queue operations over a cloud object store."""

    def __init__(self, config: RelayConfig | None = None, backend: _Backend | None = None) -> None:
        if backend is not None:
            self._backend = backend
        else:
            self._cfg = config or RelayConfig.from_env()
            self._backend = self._cfg.make_backend()

    # ---- enqueuer (home) ------------------------------------------------
    def put_pending(self, uuid: str, sealed: bytes) -> None:
        self._backend.put(f"{_PENDING}{uuid}{_SUFFIX}", sealed)

    # ---- node poller ----------------------------------------------------
    def list_pending(self) -> list[str]:
        n = len(_PENDING)
        return [
            k[n : -len(_SUFFIX)]
            for k in self._backend.list_keys(_PENDING)
            if k.endswith(_SUFFIX)
        ]

    def claim(
        self,
        uuid: str,
        owner: str,
        *,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        now: float | None = None,
    ) -> bool:
        """Atomically claim a job with a lease. True = this caller owns it now.

        Fresh claim = conditional create on ``claimed/<uuid>`` (exactly-once).
        If a claim already exists, it is taken over only when older than
        ``lease_seconds`` (the prior worker is presumed dead), via a
        compare-and-swap on the object version so two reclaimers can't both
        win. A live claim returns False.
        """
        now = time.time() if now is None else now
        key = f"{_CLAIMED}{uuid}"
        body = json.dumps({"owner": owner, "claimed_at": now}).encode("utf-8")
        if self._backend.create_only(key, body):
            return True
        # Claim exists — load body + version for a safe compare-and-swap takeover.
        existing = self._backend.read_with_version(key)
        if existing is None:
            return False  # vanished — don't risk a blind overwrite
        raw, version = existing
        try:
            claimed_at = float(json.loads(raw).get("claimed_at", 0))
        except (ValueError, TypeError):
            claimed_at = 0.0  # unparseable/empty marker => treat as expired, take over
        if now - claimed_at < lease_seconds:
            return False  # lease still live
        return self._backend.cas_write(key, body, version)

    def get_pending(self, uuid: str) -> bytes:
        return self._backend.read(f"{_PENDING}{uuid}{_SUFFIX}")

    def put_terminal(self, uuid: str, sealed: bytes) -> bool:
        """Write the single terminal object for a job, create-only.

        False = a terminal object already exists (idempotent: the first writer
        wins; duplicate/late executions are dropped, and done/failed can never
        both be recorded for one uuid)."""
        return self._backend.create_only(f"{_TERMINAL}{uuid}{_SUFFIX}", sealed)

    # ---- reconciler (home) ----------------------------------------------
    def list_terminal(self) -> list[str]:
        n = len(_TERMINAL)
        return [
            k[n : -len(_SUFFIX)]
            for k in self._backend.list_keys(_TERMINAL)
            if k.endswith(_SUFFIX)
        ]

    def get_terminal(self, uuid: str) -> bytes:
        return self._backend.read(f"{_TERMINAL}{uuid}{_SUFFIX}")

    # ---- out-of-band status (node telemetry etc.) -----------------------
    def put_status(self, name: str, sealed: bytes) -> None:
        """Write/overwrite a status object (latest wins)."""
        self._backend.put(f"{_STATUS}{name}{_SUFFIX}", sealed)

    def get_status(self, name: str) -> bytes | None:
        """Read a status object, or None if absent."""
        return self._backend.read_opt(f"{_STATUS}{name}{_SUFFIX}")

    def purge(self, uuid: str) -> None:
        """Delete every object for a job across all prefixes. Idempotent."""
        for name in (
            f"{_PENDING}{uuid}{_SUFFIX}",
            f"{_CLAIMED}{uuid}",
            f"{_TERMINAL}{uuid}{_SUFFIX}",
        ):
            self._backend.delete(name)
