from __future__ import annotations

from pathlib import Path

import yaml

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_clickhouse_integration_job() -> None:
    data = load_gitlab_ci_config()
    assert "integration-test" in data["stages"]
    assert "integration_clickhouse" in data


def test_ci_clickhouse_integration_job_uses_service_and_command() -> None:
    data = load_gitlab_ci_config()
    job = data["integration_clickhouse"]
    assert job["stage"] == "integration-test"
    assert job["variables"]["DPONE_RUN_INTEGRATION"] == "1"
    assert job["variables"]["DPONE_IT_CH_HOST"] == "clickhouse"
    assert job["variables"]["CLICKHOUSE_USER"] == "default"
    assert job["variables"]["CLICKHOUSE_PASSWORD"] == "dpone"
    assert job["variables"]["DPONE_IT_CH_PASSWORD"] == "dpone"
    assert any(service["name"] == "clickhouse/clickhouse-server:24.8" for service in job["services"])
    script = "\n".join(job["script"])
    assert "pytest -m integration_clickhouse tests/integration/clickhouse" in script
    assert "--extra clickhouse" in load_gitlab_ci_text()


def test_integration_compose_file_exposes_clickhouse() -> None:
    compose = yaml.safe_load((ROOT / "docker" / "docker-compose.integration.yml").read_text(encoding="utf-8"))
    assert "clickhouse" in compose["services"]
    service = compose["services"]["clickhouse"]
    assert service["image"] == "clickhouse/clickhouse-server:24.8"
    ports = service["ports"]
    assert any("59000" in str(port) for port in ports)
    assert any("58123" in str(port) for port in ports)
