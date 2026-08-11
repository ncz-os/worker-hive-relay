import base64
import os
import subprocess
from pathlib import Path


def test_systemd_templates_have_no_host_specific_users_or_paths():
    root = Path(__file__).parents[2]
    units = list((root / "node_relay" / "ops").glob("*.service"))
    assert units
    for unit in units:
        text = unit.read_text()
        assert "jasonperlow" not in text
        assert "/home/relay/node-relay" not in text
        assert "User=@NODE_RELAY_USER@" in text
        assert "WorkingDirectory=@NODE_RELAY_REPO@" in text
        assert "EnvironmentFile=@NODE_RELAY_ENV@" in text
        assert "ExecStart=@NODE_RELAY_PYTHON@" in text


def test_setup_migrates_legacy_key_without_rotation(tmp_path):
    root = Path(__file__).parents[2]
    config = tmp_path / "config"
    config.mkdir()
    env_file = config / "relay.env"
    legacy_key = base64.b64encode(b"L" * 32).decode()
    env_file.write_text(
        f"SPARK_HIVE_RELAY_E2EE_KEY={legacy_key}\nNODE_RELAY_E2EE_KEY=\n"
    )

    result = subprocess.run(
        ["bash", str(root / "node_relay" / "setup_relay.sh"), "env-only"],
        env={
            **os.environ,
            "NODE_RELAY_CONFIG_DIR": str(config),
            "PYTHON": os.environ.get("PYTHON", "python3"),
        },
        capture_output=True,
        text=True,
        check=True,
    )

    lines = env_file.read_text().splitlines()
    assert lines.count(f"NODE_RELAY_E2EE_KEY={legacy_key}") == 1
    assert "migrating legacy" in result.stdout


def test_setup_appends_primary_when_legacy_file_has_no_primary_line(tmp_path):
    root = Path(__file__).parents[2]
    config = tmp_path / "config"
    config.mkdir()
    env_file = config / "relay.env"
    legacy_key = base64.b64encode(b"L" * 32).decode()
    env_file.write_text(f"SPARK_HIVE_RELAY_E2EE_KEY={legacy_key}\n")

    subprocess.run(
        ["bash", str(root / "node_relay" / "setup_relay.sh"), "env-only"],
        env={**os.environ, "NODE_RELAY_CONFIG_DIR": str(config)},
        capture_output=True,
        text=True,
        check=True,
    )

    assert env_file.read_text().splitlines().count(
        f"NODE_RELAY_E2EE_KEY={legacy_key}"
    ) == 1
