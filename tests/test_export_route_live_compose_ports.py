"""Tests for fail-closed route-live Compose port discovery."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "tools" / "ci" / "export_route_live_compose_ports.py"


def _load_helper():
    spec = importlib.util.spec_from_file_location("export_route_live_compose_ports", HELPER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_compose_port_accepts_one_ipv4_loopback_mapping() -> None:
    helper = _load_helper()

    assert helper.parse_compose_port("127.0.0.1:49152\n") == 49152


@pytest.mark.parametrize(
    "output",
    (
        "0.0.0.0:49152\n",
        "[::1]:49152\n",
        "127.0.0.1:49152\n127.0.0.1:49153\n",
        "127.0.0.1:0\n",
        "127.0.0.1:65536\n",
        "127.0.0.1:not-a-port\n",
        "",
    ),
)
def test_parse_compose_port_rejects_unsafe_or_ambiguous_mapping(output: str) -> None:
    helper = _load_helper()

    with pytest.raises(ValueError):
        helper.parse_compose_port(output)


def test_discover_ports_rejects_duplicate_host_assignments() -> None:
    helper = _load_helper()

    with pytest.raises(ValueError, match="duplicate"):
        helper.discover_environment(
            ("postgres", "mssql"),
            read_port=lambda _service, _container_port: "127.0.0.1:49152\n",
        )


def test_export_does_not_mutate_github_env_when_discovery_fails(tmp_path: Path) -> None:
    helper = _load_helper()
    github_env = tmp_path / "github_env"
    github_env.write_text("EXISTING=value\n", encoding="utf-8")

    def read_port(service: str, _container_port: int) -> str:
        if service == "mssql":
            return "0.0.0.0:49153\n"
        return "127.0.0.1:49152\n"

    with pytest.raises(ValueError):
        helper.export_environment(("postgres", "mssql"), github_env=github_env, read_port=read_port)

    assert github_env.read_text(encoding="utf-8") == "EXISTING=value\n"


def test_export_appends_all_runtime_aliases_after_complete_discovery(tmp_path: Path) -> None:
    helper = _load_helper()
    github_env = tmp_path / "github_env"
    github_env.write_text("EXISTING=value\n", encoding="utf-8")
    observed = {
        "postgres": "127.0.0.1:49152\n",
        "postgis": "127.0.0.1:49153\n",
        "postgres-authority-primary": "127.0.0.1:49154\n",
        "postgres-authority-standby": "127.0.0.1:49155\n",
        "mssql": "127.0.0.1:49156\n",
    }

    assignments = helper.export_environment(
        tuple(observed),
        github_env=github_env,
        read_port=lambda service, _container_port: observed[service],
    )

    assert assignments["DPONE_IT_POSTGRES_PORT"] == 49152
    assert assignments["DPONE_IT_POSTGIS_PORT"] == 49153
    assert assignments["DPONE_IT_PG_AUTHORITY_PRIMARY_PORT_FORWARD"] == 49154
    assert assignments["DPONE_IT_PG_AUTHORITY_STANDBY_PORT_FORWARD"] == 49155
    assert assignments["DPONE_IT_MSSQL_PORT"] == 49156
    assert assignments["DPONE_IT_MSSQL_CDC_PORT"] == 49156
    content = github_env.read_text(encoding="utf-8")
    assert content.startswith("EXISTING=value\n")
    assert content.count("DPONE_IT_PG_PORT_FORWARD=49152\n") == 1
    assert content.count("DPONE_IT_MSSQL_PORT_FORWARD=49156\n") == 1


def test_discover_ports_rejects_unknown_or_repeated_services() -> None:
    helper = _load_helper()

    def reader(_service: str, _container_port: int) -> str:
        return "127.0.0.1:49152\n"

    with pytest.raises(ValueError, match="unsupported"):
        helper.discover_environment(("mysql",), read_port=reader)
    with pytest.raises(ValueError, match="repeated"):
        helper.discover_environment(("postgres", "postgres"), read_port=reader)
