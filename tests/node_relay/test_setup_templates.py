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
