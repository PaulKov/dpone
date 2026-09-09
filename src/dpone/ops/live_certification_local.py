"""Shared commands for disposable local live-certification services."""

from __future__ import annotations

from pathlib import Path

LOCAL_CERTIFICATION_SERVICES = (
    "postgres",
    "mysql",
    "mssql",
    "clickhouse",
    "kafka",
    "schema-registry",
    "minio",
)
LOCAL_SERVICE_MARKER_TESTS = (
    "tests/integration/mysql/test_mysql_source_integration.py",
    "tests/integration/postgres/test_postgres_connector_integration.py",
    "tests/integration/mssql/test_mssql_optional_integration.py",
    "tests/integration/kafka/test_kafka_optional_integration.py",
)
# The release-candidate authority freezes the eight exact cases selected by the
# four files above.  Keep the producer gate equally strict so its JUnit receipt
# cannot certify a smaller subset than the authority consumes.
LOCAL_SERVICE_MARKER_MIN_PASSED = 8
LOCAL_SERVICE_MARKERS_JUNIT = Path("test_artifacts/live_certification/local_service_markers_junit.xml")
LOCAL_SERVICE_MARKERS_EVIDENCE = Path("test_artifacts/live_certification/service_markers.json")


def start_local_services_command() -> str:
    """Return the canonical disposable-service startup command."""

    services = " ".join(LOCAL_CERTIFICATION_SERVICES)
    return f"docker compose -f docker/docker-compose.integration.yml up -d {services}"


def prepare_local_mssql_database_command() -> str:
    """Return a credential-safe command that prepares the disposable MSSQL database."""

    return (
        "docker exec dpone-it-mssql bash -c "
        """'exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa """
        """-P "$MSSQL_SA_PASSWORD" -C -b -Q "$1"' """
        "_ \"IF DB_ID(N'dpone') IS NULL CREATE DATABASE [dpone]\""
    )


def local_service_markers_command(*, profile: str = "local_live") -> str:
    """Return the fail-closed local marker command shared by every local profile."""

    selected_tests = " ".join(LOCAL_SERVICE_MARKER_TESTS)
    local_environment = (
        "DPONE_RUN_INTEGRATION=1 "
        "DPONE_IT_MYSQL_HOST=127.0.0.1 "
        "DPONE_IT_MYSQL_PORT_FORWARD=53306 "
        "DPONE_IT_MSSQL_HOST=127.0.0.1 "
        "DPONE_IT_MSSQL_PORT=51433 "
        "DPONE_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:59092 "
        "DPONE_SCHEMA_REGISTRY_URL=http://127.0.0.1:58081"
    )
    return (
        f"{local_environment} uv run pytest {selected_tests} -q -rs "
        f"--junitxml={LOCAL_SERVICE_MARKERS_JUNIT} "
        "&& uv run python tools/ci/assert_junit_executed.py "
        f"--junit {LOCAL_SERVICE_MARKERS_JUNIT} --min-passed {LOCAL_SERVICE_MARKER_MIN_PASSED} --max-skipped 0"
        f" --evidence-json {LOCAL_SERVICE_MARKERS_EVIDENCE}"
        f' --profile {profile} --commit-sha "$(git rev-parse HEAD)"'
    )
