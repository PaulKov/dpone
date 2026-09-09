from __future__ import annotations

from dpone.ops.live_certification_models import LiveCertificationStep


def native_transfer_steps(
    *,
    row_count: int,
    start_services_command: str,
    prepare_mssql_command: str,
    service_markers_command: str,
) -> tuple[LiveCertificationStep, ...]:
    """Build the native-transfer live certification chain.

    PostgreSQL -> MSSQL release evidence is owned by the exhaustive governed
    route workflow. The lightweight direct-sink fixtures predate mandatory
    receipt/fence admission and must not be projected as positive evidence.
    """

    del row_count
    return (
        LiveCertificationStep(
            "install_native_tooling",
            "Install unixODBC, Microsoft ODBC Driver 18, mssql-tools18/bcp, clickhouse-client, and docker compose.",
            tuple(),
            required=False,
        ),
        LiveCertificationStep(
            "start_local_services",
            start_services_command,
            tuple(),
        ),
        LiveCertificationStep(
            "prepare_local_mssql_database",
            prepare_mssql_command,
            tuple(),
        ),
        LiveCertificationStep(
            "run_local_service_markers",
            service_markers_command,
            ("local_service_markers_junit.xml", "service_markers.json"),
        ),
        LiveCertificationStep(
            "run_native_transfer_live_fixtures",
            "uv run pytest tests/integration/mssql/test_mssql_to_clickhouse_native_transfer_integration.py -q "
            "--junitxml=test_artifacts/live_certification/native_transfer_live_junit.xml && "
            "uv run python tools/ci/assert_junit_executed.py "
            "--junit test_artifacts/live_certification/native_transfer_live_junit.xml "
            "--min-passed 3 --max-skipped 0 --evidence-json "
            "test_artifacts/live_certification/native_transfer_live_fixtures.json "
            '--profile native_transfer --commit-sha "$(git rev-parse HEAD)"',
            ("native_transfer_live_junit.xml", "native_transfer_live_fixtures.json"),
        ),
        LiveCertificationStep(
            "stop_local_services",
            "docker compose -f docker/docker-compose.integration.yml down -v",
            tuple(),
            required=False,
        ),
    )
