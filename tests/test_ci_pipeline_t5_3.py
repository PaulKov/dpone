from __future__ import annotations

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
from pathlib import Path

import yaml

from tests.gitlab_ci import load_gitlab_ci_config

ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_declares_extended_integration_markers_and_minio_dev_dep() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    markers = data["tool"]["pytest"]["ini_options"]["markers"]
    assert any(marker.startswith("integration_extended:") for marker in markers)
    assert any(marker.startswith("nightly:") for marker in markers)

    dev_group = data["dependency-groups"]["dev"]
    assert any(dep.startswith("minio>=") for dep in dev_group)


def test_integration_compose_file_exposes_minio() -> None:
    compose = yaml.safe_load((ROOT / "docker" / "docker-compose.integration.yml").read_text(encoding="utf-8"))
    assert "minio" in compose["services"]
    minio = compose["services"]["minio"]
    assert minio["image"] == "minio/minio:latest"
    assert any("59090" in str(port) for port in minio["ports"])
    assert any("59091" in str(port) for port in minio["ports"])


def test_ci_has_extended_clickhouse_export_job() -> None:
    data = load_gitlab_ci_config()
    job = data["integration_clickhouse_export"]
    assert job["stage"] == "integration-test"
    assert job["before_script"][-1] == "uv sync --group dev --extra clickhouse --extra gcp"
    assert job["variables"]["CI_DEBUG_SERVICES"] == "true"
    assert job["variables"]["DPONE_RUN_INTEGRATION_EXTENDED"] == "1"
    assert job["variables"]["DPONE_IT_MINIO_PORT"] == "9001"
    assert job["variables"]["DPONE_IT_MINIO_INTERNAL_URL"] == "http://minio:9001"
    services = job["services"]
    assert any(service["alias"] == "clickhouse" for service in services)
    assert any(service["alias"] == "minio" for service in services)
    minio = next(service for service in services if service["alias"] == "minio")
    assert "entrypoint" not in minio
    assert minio["command"] == ["server", "/tmp/minio-data", "--address", ":9001", "--console-address", ":9002"]
    assert minio["variables"]["MINIO_ROOT_USER"] == "minioadmin"
    assert minio["variables"]["MINIO_ROOT_PASSWORD"] == "minioadmin"
    assert minio["variables"]["HEALTHCHECK_TCP_PORT"] == "9001"
    script = "\n".join(job["script"])
    assert "pytest -m integration_extended" in script
    assert "test_clickhouse_export_integration.py" in script
    assert "getent hosts minio || true" in script
    assert "http://127.0.0.1:9001/minio/health/live" in script
    assert "http://minio:9001/minio/health/live" in script


def test_ci_has_snapshot_installed_integration_job() -> None:
    data = load_gitlab_ci_config()
    job = data["snapshot_installed_integration"]
    assert job["stage"] == "smoke-test"
    assert any(need["job"] == "publish_dev_snapshot" for need in job["needs"])
    assert job["variables"]["DPONE_TEST_USE_INSTALLED_PACKAGE"] == "1"
    services = job["services"]
    postgres = next(service for service in services if service["alias"] == "postgres")
    assert "entrypoint" not in postgres
    assert postgres["command"] == ["postgres"]
    assert postgres["variables"]["PGDATA"] == "/tmp/postgres-data"
    assert postgres["variables"]["POSTGRES_DB"] == "dpone_it"
    assert postgres["variables"]["POSTGRES_USER"] == "dpone"
    assert postgres["variables"]["POSTGRES_PASSWORD"] == "dpone"
    assert postgres["variables"]["POSTGRES_HOST_AUTH_METHOD"] == "trust"
    assert postgres["variables"]["HEALTHCHECK_TCP_PORT"] == "5432"
    assert any(service["alias"] == "clickhouse" for service in services)
    script = "\n".join(job["script"])
    assert "getent hosts postgres || true" in script
    assert '("postgres", 5432)' in script
    assert '("127.0.0.1", 5432)' in script
    assert "PostgreSQL service did not accept TCP connections" in script
    assert 'pip install pytest "$LIB_NAME[postgres,clickhouse]==$DPONE_SNAPSHOT_VERSION"' in script
    assert 'pytest -m "integration and not integration_extended"' in script
