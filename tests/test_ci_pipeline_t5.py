from __future__ import annotations

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
from pathlib import Path

import yaml

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_postgres_integration_job_and_stage() -> None:
    data = load_gitlab_ci_config()
    assert "integration-test" in data["stages"]
    assert data["stages"].index("pre-test") < data["stages"].index("integration-test") < data["stages"].index("build")
    assert "integration_postgres" in data


def test_ci_postgres_integration_job_uses_service_and_marker_command() -> None:
    data = load_gitlab_ci_config()
    job = data["integration_postgres"]
    assert job["stage"] == "integration-test"
    assert job["variables"]["DPONE_RUN_INTEGRATION"] == "1"
    assert job["variables"]["DPONE_IT_PG_HOST"] == "postgres"
    postgres = next(service for service in job["services"] if service["alias"] == "postgres")
    assert postgres["name"] == "postgres:16-alpine"
    assert "entrypoint" not in postgres
    assert postgres["command"] == ["postgres"]
    assert postgres["variables"]["PGDATA"] == "/tmp/postgres-data"
    assert postgres["variables"]["POSTGRES_DB"] == "dpone_it"
    assert postgres["variables"]["POSTGRES_USER"] == "dpone"
    assert postgres["variables"]["POSTGRES_PASSWORD"] == "dpone"
    assert postgres["variables"]["POSTGRES_HOST_AUTH_METHOD"] == "trust"
    assert postgres["variables"]["HEALTHCHECK_TCP_PORT"] == "5432"
    script = "\n".join(job["script"])
    assert "getent hosts postgres || true" in script
    assert '("postgres", 5432)' in script
    assert '("127.0.0.1", 5432)' in script
    assert "PostgreSQL service did not accept TCP connections" in script
    assert "pytest -m integration tests/integration/postgres" in script
    assert "--extra postgres" in load_gitlab_ci_text()


def test_pyproject_declares_integration_marker() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    markers = data["tool"]["pytest"]["ini_options"]["markers"]
    assert any(marker.startswith("integration:") for marker in markers)
    postgres_extra = data["project"]["optional-dependencies"]["postgres"]
    assert any(dep.startswith("psycopg[binary]==") for dep in postgres_extra)


def test_integration_compose_file_exposes_postgres() -> None:
    compose = yaml.safe_load((ROOT / "docker" / "docker-compose.integration.yml").read_text(encoding="utf-8"))
    assert "postgres" in compose["services"]
    postgres = compose["services"]["postgres"]
    assert postgres["image"] == (
        "postgres:16-alpine@sha256:44c4ee9810eff91f7eab4d822642e01115b1a9eccce4bcbdde7604752d68eac6"
    )
    assert any("55432" in port for port in postgres["ports"])
