"""node-relay repo-resolution + offload-eligibility tests.

Regression coverage for the ``build:<repo>`` mapping: the relay must understand
the home-fleet ``build:<repo>`` convention (allowlist-resolved) as well as the
legacy colon-prefixed project kinds, and must NOT offload unmappable build jobs.
Pure unit tests: no GCS / S3 / network / model needed.
"""

import pytest

from node_relay import enqueuer
from node_relay.enqueuer import _node_should_offload
from node_relay.node_poller import repo_url_for_kind, repo_aliases


class TestRepoUrlForKind:
    def test_build_prefix_resolves_known_repo(self):
        assert repo_url_for_kind("build:mnemos") == "https://gitlab.com/mnemos-os/mnemos.git"
        assert repo_url_for_kind("build:riskyeats") == "https://gitlab.com/perlowja/riskyeats.git"
        assert repo_url_for_kind("build:zeroclaw") == "https://gitlab.com/nclawzero/zeroclaw.git"

    def test_build_prefix_strips_trailing_kind_segment(self):
        assert repo_url_for_kind("build:mnemos:phase1") == "https://gitlab.com/mnemos-os/mnemos.git"

    def test_build_prefix_unknown_repo_is_unmapped(self):
        # SSRF-safe: an unknown suffix must NOT resolve.
        assert repo_url_for_kind("build:pantheon-security") is None
        assert repo_url_for_kind("build:totally-unknown") is None

    def test_legacy_colon_kinds_still_resolve(self):
        assert repo_url_for_kind("mnemos:fix-something") == "https://gitlab.com/mnemos-os/mnemos.git"
        assert repo_url_for_kind("ic-engine:patch") == "https://gitlab.com/argonautsystems/ic-engine.git"

    def test_unmapped_kind_returns_none(self):
        assert repo_url_for_kind("research:market") is None
        assert repo_url_for_kind("") is None
        assert repo_url_for_kind(None) is None

    def test_env_allowlist_extends_build_resolution(self, monkeypatch):
        monkeypatch.setenv("NODE_RELAY_REPO_ALLOWLIST", "widget=https://example.com/acme/widget.git")
        assert "widget" in repo_aliases()
        assert repo_url_for_kind("build:widget") == "https://example.com/acme/widget.git"

    def test_legacy_env_allowlist_still_honored(self, monkeypatch):
        monkeypatch.delenv("NODE_RELAY_REPO_ALLOWLIST", raising=False)
        monkeypatch.setenv("SPARK_REPO_ALLOWLIST", "legacy=https://example.com/acme/legacy.git")
        assert repo_url_for_kind("build:legacy") == "https://example.com/acme/legacy.git"


class TestNodeShouldOffload:
    @pytest.fixture(autouse=True)
    def _host(self, monkeypatch):
        # _node_should_offload reads the module-level NODE_HOST identity.
        monkeypatch.setattr(enqueuer, "NODE_HOST", "node-x")

    def test_mappable_build_job_is_offloaded(self):
        assert _node_should_offload({"kind": "build:mnemos"}) is True
        assert _node_should_offload({"kind": "build:riskyeats"}) is True

    def test_unmappable_build_job_is_released(self):
        assert _node_should_offload({"kind": "build:pantheon-security"}) is False

    def test_noncommit_kind_offloaded_without_repo(self):
        assert _node_should_offload({"kind": "research:foo"}) is True
        assert _node_should_offload({"kind": "analysis:bar"}) is True

    def test_explicit_host_target_always_offloaded(self):
        assert _node_should_offload({"kind": "build:anything", "eligible_hosts": ["node-x"]}) is True

    def test_unmapped_unhosted_commit_kind_released(self):
        assert _node_should_offload({"kind": "build:unknown"}) is False
        assert _node_should_offload({"kind": "fix:somerepo"}) is False


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
