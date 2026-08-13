"""mnemos-node-relay repo-resolution + offload-eligibility tests.

Regression coverage for the ``build:<repo>`` mapping: the relay must understand
the home-fleet ``build:<repo>`` convention (allowlist-resolved) as well as the
legacy colon-prefixed project kinds, and must NOT offload unmappable build jobs.
Pure unit tests: no GCS / S3 / network / model needed.
"""

import pytest

from node_relay.lander import PatchLander
from node_relay import enqueuer
from node_relay.enqueuer import _node_should_offload
from node_relay.node_poller import (
    AgenticRepoExecutor,
    FallbackExecutor,
    make_executor,
    repo_aliases,
    repo_url_for_kind,
)


class _DummyChat:
    timeout = 1.0
    default_model = "dummy"

    def complete(self, **kwargs):  # pragma: no cover - not used by these tests
        raise AssertionError("unexpected model call")


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

    def test_host_target_does_not_replace_missing_commit_workspace(self):
        assert _node_should_offload({"kind": "code", "eligible_hosts": ["node-x"]}) is False

    @pytest.mark.parametrize("field", ["project", "target_workspace"])
    def test_host_targeted_commit_with_emitted_workspace_is_offloaded(self, field):
        assert _node_should_offload(
            {"kind": "code", "eligible_hosts": ["NODE-X"], field: "mnemos"}
        ) is True

    def test_host_targeted_noncommit_still_offloaded_without_workspace(self):
        assert _node_should_offload(
            {"kind": "research:anything", "eligible_hosts": ["node-x"]}
        ) is True

    @pytest.mark.parametrize(
        "repo_fields",
        [
            {"repo": "https://gitlab.com/mnemos-os/mnemos.git"},
            {"description": "make the change\nrepo: mnemos"},
        ],
    )
    def test_host_targeted_existing_repo_contracts_remain_offloadable(self, repo_fields):
        assert _node_should_offload(
            {"kind": "code", "eligible_hosts": ["node-x"], **repo_fields}
        ) is True

    def test_unmapped_unhosted_commit_kind_released(self):
        assert _node_should_offload({"kind": "build:unknown"}) is False
        assert _node_should_offload({"kind": "fix:somerepo"}) is False


class TestGitCredentialHandling:
    def test_gitlab_token_uses_askpass_not_clone_url(self, monkeypatch, tmp_path):
        monkeypatch.setenv("GITLAB_TOKEN", "secret-token")
        ex = AgenticRepoExecutor(_DummyChat())

        url, env = ex._git_auth_for_url("https://gitlab.com/mnemos-os/mnemos.git", tmp_path)

        assert url == "https://gitlab.com/mnemos-os/mnemos.git"
        assert "secret-token" not in url
        assert env is not None
        assert env["NODE_RELAY_GIT_PASSWORD"] == "secret-token"
        assert env["GIT_TERMINAL_PROMPT"] == "0"
        assert env["GIT_CONFIG_KEY_0"] == "credential.helper"
        assert env["GIT_CONFIG_VALUE_0"] == ""
        assert (tmp_path / "git-askpass.sh").stat().st_mode & 0o777 == 0o700

    def test_token_not_attached_for_non_fleet_owner(self, monkeypatch, tmp_path):
        monkeypatch.setenv("GITLAB_TOKEN", "secret-token")
        ex = AgenticRepoExecutor(_DummyChat())

        url, env = ex._git_auth_for_url("https://gitlab.com/attacker/project.git", tmp_path)

        assert url == "https://gitlab.com/attacker/project.git"
        assert env is None

    def test_embedded_credentials_are_rejected(self, tmp_path):
        ex = AgenticRepoExecutor(_DummyChat())

        with pytest.raises(RuntimeError, match="embedded credentials"):
            ex._git_auth_for_url("https://oauth2:secret@gitlab.com/mnemos-os/mnemos.git", tmp_path)


class TestRepositoryExecutorPolicy:
    def test_delete_unlinks_repository_symlink_not_its_target(self, tmp_path):
        ex = AgenticRepoExecutor(_DummyChat())
        target = tmp_path / "configs" / "prod.conf"
        target.parent.mkdir()
        target.write_text("production=true\n")
        link = tmp_path / "current.conf"
        link.symlink_to("configs/prod.conf")

        assert ex._write_file_changes(tmp_path, {"current.conf": None}) == []
        assert not link.exists()
        assert not link.is_symlink()
        assert target.read_text() == "production=true\n"

    def test_replacement_of_repository_symlink_is_rejected(self, tmp_path):
        ex = AgenticRepoExecutor(_DummyChat())
        target = tmp_path / "target.txt"
        target.write_text("original\n")
        link = tmp_path / "link.txt"
        link.symlink_to("target.txt")

        assert ex._write_file_changes(tmp_path, {"link.txt": "replacement"}) == ["link.txt"]
        assert link.is_symlink()
        assert target.read_text() == "original\n"

    def test_local_policy_keeps_repository_jobs_local(self, monkeypatch):
        monkeypatch.setenv("CLOUD_LLM_BASE", "https://cloud.example/v1")
        executor = make_executor("local")

        assert executor.repo.chat.label == "local"

    def test_explicit_allowlisted_repo_url_is_honored(self):
        ex = AgenticRepoExecutor(_DummyChat())

        assert ex._resolve_repo_url(
            {"kind": "build:custom", "repo": "https://gitlab.com/mnemos-os/mnemos.git"}
        ) == "https://gitlab.com/mnemos-os/mnemos.git"
        assert ex._resolve_repo_url(
            {"kind": "build:custom", "repo": "https://attacker.example/repo.git"}
        ) is None

    def test_structured_workspace_alias_is_honored(self):
        ex = AgenticRepoExecutor(_DummyChat())

        assert ex._resolve_repo_url(
            {"kind": "build:custom", "workspace": "mnemos"}
        ) == "https://gitlab.com/mnemos-os/mnemos.git"

    def test_pinned_model_is_used_for_both_agentic_calls(self):
        calls = []

        class Chat(_DummyChat):
            def complete(self, **kwargs):
                calls.append(kwargs)
                return "file.py" if len(calls) == 1 else ""

        ex = AgenticRepoExecutor(Chat())
        job = {"prompt": "change it", "model": "submitter-model"}
        ex._request_file_list(job, "file.py")
        ex._request_changes(job, "file.py", "")

        assert [call["model"] for call in calls] == ["submitter-model", "submitter-model"]

    def test_fallback_bypasses_failed_backend_for_rest_of_job(self):
        calls = []

        class Chat(_DummyChat):
            def __init__(self, label, fails=False):
                self.label = label
                self.default_model = label
                self.timeout = 1
                self.fails = fails

            def complete(self, **kwargs):
                calls.append(self.label)
                if self.fails:
                    raise TimeoutError(self.label)
                return "ok"

        fallback = FallbackExecutor([("local", Chat("local", True)), ("cloud", Chat("cloud"))])
        fallback.begin_job()
        assert fallback.complete(user="first") == "ok"
        assert fallback.complete(user="second") == "ok"
        fallback.end_job()
        assert fallback.complete(user="next-job") == "ok"
        assert calls == ["local", "cloud", "cloud", "local", "cloud"]


def test_lander_cache_cap_evicts_oldest_unused_clone(monkeypatch, tmp_path):
    monkeypatch.setenv("NODE_RELAY_LANDER_CACHE_MAX_BYTES", "10")
    lander = PatchLander(cache_dir=str(tmp_path))
    old = tmp_path / "old"
    current = tmp_path / "current"
    for clone in (old, current):
        (clone / ".git").mkdir(parents=True)
        (clone / "payload").write_bytes(b"123456")

    lander._prune_cache(exclude=current)

    assert not old.exists()
    assert current.exists()
    assert lander.orphan_dir.exists()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
