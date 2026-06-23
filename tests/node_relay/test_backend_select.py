"""node-relay backend selection: gs://, s3://, bare name, explicit override,
and legacy env fallback. Pure config logic — constructs no live client."""

import pytest

from node_relay.relay_client import RelayConfig


def _clear(monkeypatch):
    for v in (
        "NODE_RELAY_BUCKET", "SPARK_HIVE_RELAY_BUCKET", "NODE_RELAY_BACKEND",
        "NODE_RELAY_GCS_SA", "SPARK_HIVE_RELAY_GCS_SA",
        "NODE_RELAY_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION", "NODE_RELAY_S3_ENDPOINT",
    ):
        monkeypatch.delenv(v, raising=False)


def test_bare_name_defaults_gcs(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("NODE_RELAY_BUCKET", "my-bucket")
    cfg = RelayConfig.from_env()
    assert cfg.bucket == "my-bucket"
    assert cfg.backend == "gcs"


def test_gs_scheme_infers_gcs(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("NODE_RELAY_BUCKET", "gs://my-bucket")
    cfg = RelayConfig.from_env()
    assert cfg.bucket == "my-bucket"
    assert cfg.backend == "gcs"


def test_s3_scheme_infers_s3(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("NODE_RELAY_BUCKET", "s3://my-bucket")
    cfg = RelayConfig.from_env()
    assert cfg.bucket == "my-bucket"
    assert cfg.backend == "s3"


def test_explicit_backend_overrides_scheme(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("NODE_RELAY_BUCKET", "gs://my-bucket")
    monkeypatch.setenv("NODE_RELAY_BACKEND", "s3")
    cfg = RelayConfig.from_env()
    assert cfg.backend == "s3"


def test_legacy_bucket_env_fallback(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("SPARK_HIVE_RELAY_BUCKET", "old-bucket")
    cfg = RelayConfig.from_env()
    assert cfg.bucket == "old-bucket"


def test_unknown_backend_rejected(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("NODE_RELAY_BUCKET", "b")
    monkeypatch.setenv("NODE_RELAY_BACKEND", "azure")
    with pytest.raises(RuntimeError):
        RelayConfig.from_env()


def test_missing_bucket_raises(monkeypatch):
    _clear(monkeypatch)
    with pytest.raises(RuntimeError):
        RelayConfig.from_env()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
