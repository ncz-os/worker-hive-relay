"""node-relay E2EE crypto tests: roundtrip, AAD binding, key handling, and
backward-compatible decryption of the legacy ``SHR1`` magic."""

import base64
import os

import pytest

from node_relay import relay_crypto


KEY = base64.b64encode(b"0" * 32).decode()


def _key() -> bytes:
    return relay_crypto.load_key(KEY)


class TestLoadKey:
    def test_loads_from_arg(self):
        assert relay_crypto.load_key(KEY) == b"0" * 32

    def test_prefers_new_env_then_legacy(self, monkeypatch):
        monkeypatch.delenv("NODE_RELAY_E2EE_KEY", raising=False)
        monkeypatch.setenv("SPARK_HIVE_RELAY_E2EE_KEY", KEY)
        assert relay_crypto.load_key() == b"0" * 32  # legacy fallback works
        monkeypatch.setenv("NODE_RELAY_E2EE_KEY", base64.b64encode(b"1" * 32).decode())
        assert relay_crypto.load_key() == b"1" * 32  # new name wins

    def test_missing_key_raises(self, monkeypatch):
        monkeypatch.delenv("NODE_RELAY_E2EE_KEY", raising=False)
        monkeypatch.delenv("SPARK_HIVE_RELAY_E2EE_KEY", raising=False)
        with pytest.raises(relay_crypto.RelayCryptoError):
            relay_crypto.load_key()

    def test_bad_length_raises(self):
        with pytest.raises(relay_crypto.RelayCryptoError):
            relay_crypto.load_key(base64.b64encode(b"short").decode())


class TestSealOpen:
    def test_roundtrip(self):
        key = _key()
        payload = {"job_id": "abc", "prompt": "hello", "context": [{"id": 1, "content": "x"}]}
        blob = relay_crypto.seal(payload, key, aad=relay_crypto.aad_for("pending", "abc"))
        assert blob[:4] == relay_crypto.MAGIC  # writes the current magic
        got = relay_crypto.open_blob(blob, key, aad=relay_crypto.aad_for("pending", "abc"))
        assert got == payload

    def test_aad_mismatch_fails(self):
        key = _key()
        blob = relay_crypto.seal({"x": 1}, key, aad=relay_crypto.aad_for("pending", "abc"))
        with pytest.raises(relay_crypto.RelayCryptoError):
            # same uuid, different prefix => cross-prefix replay rejected
            relay_crypto.open_blob(blob, key, aad=relay_crypto.aad_for("terminal", "abc"))

    def test_wrong_key_fails(self):
        blob = relay_crypto.seal({"x": 1}, _key(), aad=relay_crypto.aad_for("pending", "a"))
        other = relay_crypto.load_key(base64.b64encode(b"9" * 32).decode())
        with pytest.raises(relay_crypto.RelayCryptoError):
            relay_crypto.open_blob(blob, other, aad=relay_crypto.aad_for("pending", "a"))

    def test_tamper_fails(self):
        key = _key()
        blob = bytearray(relay_crypto.seal({"x": 1}, key, aad=relay_crypto.aad_for("pending", "a")))
        blob[-1] ^= 0xFF  # flip a ciphertext/tag byte
        with pytest.raises(relay_crypto.RelayCryptoError):
            relay_crypto.open_blob(bytes(blob), key, aad=relay_crypto.aad_for("pending", "a"))

    def test_write_legacy_magic_flag_roundtrips(self, monkeypatch):
        """With the migration flag set, seal() writes SHR1 (so un-migrated
        readers can decrypt) and this reader still opens it."""
        key = _key()
        monkeypatch.setenv("NODE_RELAY_WRITE_LEGACY_MAGIC", "1")
        blob = relay_crypto.seal({"x": 1}, key, aad=relay_crypto.aad_for("pending", "a"))
        assert blob[:4] == relay_crypto.LEGACY_MAGIC
        got = relay_crypto.open_blob(blob, key, aad=relay_crypto.aad_for("pending", "a"))
        assert got == {"x": 1}

    def test_legacy_magic_still_opens(self):
        """A blob written with the old SHR1 magic must still decrypt (in-flight
        objects survive the rename)."""
        key = _key()
        # Forge a legacy blob using the same primitives the old code used: the
        # AAD bound the OLD header, so re-create that exactly.
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        nonce = b"\x00" * 12
        legacy_header = b"SHR1" + bytes([relay_crypto.VERSION])
        aad = legacy_header + relay_crypto.aad_for("pending", "a")
        import json

        ct = AESGCM(key).encrypt(nonce, json.dumps({"x": 1}, separators=(",", ":")).encode(), aad)
        legacy_blob = legacy_header + nonce + ct
        got = relay_crypto.open_blob(legacy_blob, key, aad=relay_crypto.aad_for("pending", "a"))
        assert got == {"x": 1}


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
