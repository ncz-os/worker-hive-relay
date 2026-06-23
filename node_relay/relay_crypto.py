"""End-to-end encryption for the node-relay cloud-object transport.

The relay bucket (GCS or S3) is a transport only; it must never see plaintext
job payloads or MNEMOS context. Both endpoints — the home-fleet enqueuer/
reconciler and the remote/airgapped node poller — share one symmetric
AES-256-GCM key (``NODE_RELAY_E2EE_KEY``, base64, 32 bytes). The cloud provider
sees only ciphertext.

Wire format of a sealed blob::

    magic(4) || version(1) || nonce(12) || ciphertext+tag(...)

``magic`` = ``b"NDR1"`` lets us reject foreign / corrupt objects early; the GCM
tag (appended to the ciphertext by ``AESGCM.encrypt``) authenticates both the
ciphertext and the magic+version header (passed as associated data). For
zero-downtime migration the reader also accepts the legacy ``b"SHR1"`` magic
that earlier relay versions wrote, so blobs already in flight decrypt cleanly.
"""

from __future__ import annotations

import base64
import json
import os
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MAGIC = b"NDR1"
# Legacy magic written by the spark-relay generation of this code. Always
# accepted on read so in-flight objects survive the rename. It can also be
# WRITTEN (see ``_write_magic``) during a phased migration: if the home side is
# upgraded to node-relay before a remote node is, set
# ``NODE_RELAY_WRITE_LEGACY_MAGIC=1`` so the still-old node — which only reads
# SHR1 — keeps decrypting. Drop the flag once every participant is upgraded.
LEGACY_MAGIC = b"SHR1"
_LEGACY_MAGICS = (LEGACY_MAGIC,)
VERSION = 1
_NONCE_LEN = 12
_HEADER = MAGIC + bytes([VERSION])
# Primary env var, with backward-compatible fallback to the pre-rename name so a
# host whose EnvironmentFile still sets the old key keeps working unchanged.
_KEY_ENVS = ("NODE_RELAY_E2EE_KEY", "SPARK_HIVE_RELAY_E2EE_KEY")


class RelayCryptoError(Exception):
    """Raised when a blob cannot be sealed or opened."""


def _key_from_env() -> str | None:
    for name in _KEY_ENVS:
        val = os.environ.get(name)
        if val:
            return val
    return None


def load_key(b64_key: str | None = None) -> bytes:
    """Load the 32-byte AES key from arg or ``NODE_RELAY_E2EE_KEY``.

    Falls back to the legacy ``SPARK_HIVE_RELAY_E2EE_KEY`` so an un-migrated
    host keeps working without an env edit.
    """
    raw = b64_key if b64_key is not None else _key_from_env()
    if not raw:
        raise RelayCryptoError(
            f"no E2EE key: pass one or set ${_KEY_ENVS[0]} (base64 of 32 bytes)"
        )
    try:
        key = base64.b64decode(raw, validate=True)
    except (ValueError, base64.binascii.Error) as exc:  # type: ignore[attr-defined]
        raise RelayCryptoError(f"E2EE key is not valid base64: {exc}") from exc
    if len(key) != 32:
        raise RelayCryptoError(f"E2EE key must decode to 32 bytes (AES-256), got {len(key)}")
    return key


def aad_for(kind: str, uuid: str) -> bytes:
    """Associated-data context binding a blob to its bucket prefix + job id.

    Passed to :func:`seal`/:func:`open_blob` so a ciphertext authenticated for
    e.g. ``pending/<uuid>`` cannot be replayed as ``terminal/<uuid>`` or moved
    to a different job — the GCM tag covers ``kind`` and ``uuid``, so any
    mismatch fails authentication. ``kind`` is one of
    ``pending``/``terminal``/``status``.
    """
    return f"{kind}:{uuid}".encode("utf-8")


def _write_magic() -> bytes:
    """Magic this process WRITES. Defaults to the current ``NDR1``; set
    ``NODE_RELAY_WRITE_LEGACY_MAGIC`` truthy to emit the legacy ``SHR1`` for
    compatibility with un-migrated readers during a phased rollout."""
    flag = (os.environ.get("NODE_RELAY_WRITE_LEGACY_MAGIC") or "").strip().lower()
    return LEGACY_MAGIC if flag in ("1", "true", "yes", "on") else MAGIC


def _aad_with_magic(magic: bytes, context: bytes | None) -> bytes:
    # The framing header is authenticated; per-object context (if any) is
    # appended so it is bound into the same tag. The reader reconstructs the
    # matching header from the blob's own magic, so a blob written under either
    # magic verifies under its own header.
    return magic + bytes([VERSION]) + (context or b"")


def seal(
    payload: dict[str, Any],
    key: bytes,
    *,
    aad: bytes | None = None,
    nonce: bytes | None = None,
) -> bytes:
    """Serialize ``payload`` to JSON and encrypt it. Returns the sealed blob.

    ``aad`` binds the ciphertext to a context (see :func:`aad_for`); the same
    value must be supplied to :func:`open_blob`. ``nonce`` is injectable for
    tests only; production always uses a fresh random 96-bit nonce (GCM requires
    nonce uniqueness per key).
    """
    if len(key) != 32:
        raise RelayCryptoError("key must be 32 bytes")
    if nonce is None:
        nonce = os.urandom(_NONCE_LEN)
    elif len(nonce) != _NONCE_LEN:
        raise RelayCryptoError(f"nonce must be {_NONCE_LEN} bytes")
    magic = _write_magic()
    plaintext = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, _aad_with_magic(magic, aad))
    return magic + bytes([VERSION]) + nonce + ciphertext


def open_blob(blob: bytes, key: bytes, *, aad: bytes | None = None) -> dict[str, Any]:
    """Decrypt and JSON-decode a sealed blob. Inverse of :func:`seal`.

    ``aad`` must match the value passed to :func:`seal`, else authentication
    fails (defends against cross-prefix / cross-job replay). Both the current
    ``NDR1`` magic and the legacy ``SHR1`` magic are accepted on read.
    """
    if len(blob) < len(_HEADER) + _NONCE_LEN:
        raise RelayCryptoError("blob too short")
    magic = blob[: len(MAGIC)]
    if magic != MAGIC and magic not in _LEGACY_MAGICS:
        raise RelayCryptoError("bad magic — not a node-relay blob")
    version = blob[len(MAGIC)]
    if version != VERSION:
        raise RelayCryptoError(f"unsupported relay version {version}")
    nonce = blob[len(_HEADER) : len(_HEADER) + _NONCE_LEN]
    ciphertext = blob[len(_HEADER) + _NONCE_LEN :]
    try:
        plaintext = AESGCM(key).decrypt(nonce, ciphertext, _aad_with_magic(magic, aad))
    except InvalidTag as exc:
        raise RelayCryptoError("authentication failed — wrong key or tampered blob") from exc
    return json.loads(plaintext)
