"""Live/local certification automation planning."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from dpone.contracts import LIVE_CERTIFICATION_PROFILE_CHOICES, LOCAL_LIVE_CERTIFICATION_PROFILES
from dpone.ops.live_certification_local import (
    LOCAL_CERTIFICATION_SERVICES,
    local_service_markers_command,
    prepare_local_mssql_database_command,
    start_local_services_command,
)
from dpone.ops.live_certification_models import LiveCertificationStep
from dpone.ops.live_certification_native_transfer import native_transfer_steps
from dpone.ops.managed_credentials import DEFAULT_VENDOR_LIVE_ENV

SUPPORTED_PROFILES = frozenset(LIVE_CERTIFICATION_PROFILE_CHOICES)
# MySQL -> MSSQL is intentionally absent: target-derived MySQL watermarks are
# fail-closed for MSSQL, and the old direct-sink fixtures bypass the mandatory
# external transaction catalog. They are contract tests, not positive route
# certification evidence.
_MYSQL_LOCAL_ROUTE_CELLS = {
    "postgres": ("full_refresh_csv_copy", "incremental_merge_watermark"),
    "clickhouse": ("full_refresh_tsv", "incremental_merge_watermark"),
    "kafka": ("full_refresh_csv_produce",),
}
_MYSQL_ROUTE_JUNIT = Path("test_artifacts/live_certification/mysql/mysql_local_route_cells_junit.xml")
_MYSQL_ROUTE_EVIDENCE = Path("test_artifacts/live_certification/mysql/mysql_local_route_cells.json")


@dataclass(frozen=True, slots=True)
class LiveCertificationAutomationReport:
    profile: str
    row_count: int
    passed: bool
    credentials_required: bool
    services: tuple[str, ...]
    required_environment: tuple[str, ...]
    required_artifacts: tuple[str, ...]
    steps: tuple[LiveCertificationStep, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "row_count": self.row_count,
            "passed": self.passed,
            "credentials_required": self.credentials_required,
            "services": list(self.services),
            "required_environment": list(self.required_environment),
            "required_artifacts": list(self.required_artifacts),
            "steps": [step.to_dict() for step in self.steps],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone live certification automation plan",
            "",
            f"- Profile: `{self.profile}`",
            f"- Row count: `{self.row_count}`",
            f"- Passed: `{self.passed}`",
            f"- Credentials required: `{self.credentials_required}`",
            f"- Services: `{', '.join(self.services) or '-'}`",
            f"- Required environment: `{', '.join(self.required_environment) or '-'}`",
            "",
            "| step | required | artifacts | command |",
            "|---|---|---|---|",
        ]
        for step in self.steps:
            artifacts = ", ".join(f"`{artifact}`" for artifact in step.artifacts)
            lines.append(f"| `{step.name}` | `{step.required}` | {artifacts} | `{step.command}` |")
        lines.extend(
            [
                "",
                "## Runbook",
                "",
                "1. Run local service certification with disposable Docker services first.",
                "2. Use `vendor_live` only when secrets and cost controls are explicitly configured.",
                "3. Treat absent performance, state, reconciliation, nested-live, or package evidence as `UNVERIFIED`.",
                "4. Upload the whole `test_artifacts/live_certification/` directory on success and failure.",
                "",
            ]
        )
        return "\n".join(lines)


class LiveCertificationAutomationService:
    """Builds a deterministic plan for local-live and vendor-live certification."""

    def build(
        self,
        *,
        output_dir: str | Path,
        profile: str = "local_live",
        row_count: int = 10000,
        include_vendor_live: bool = False,
    ) -> LiveCertificationAutomationReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        normalized_profile = profile if profile in SUPPORTED_PROFILES else "local_live"
        normalized_row_count = max(1, min(int(row_count), 100000))
        credentials_required = normalized_profile == "vendor_live" and include_vendor_live
        services = LOCAL_CERTIFICATION_SERVICES if normalized_profile in LOCAL_LIVE_CERTIFICATION_PROFILES else tuple()
        required_environment = ("vendor_live_secrets",) if credentials_required else tuple()
        steps = self._steps(
            profile=normalized_profile,
            row_count=normalized_row_count,
            include_vendor_live=include_vendor_live,
        )
        required_artifacts = tuple(
            dict.fromkeys(artifact for step in steps if step.required for artifact in step.artifacts)
        )
        json_path = directory / "live_certification_plan.json"
        markdown_path = directory / "live_certification_plan.md"
        report = LiveCertificationAutomationReport(
            profile=normalized_profile,
            row_count=normalized_row_count,
            passed=True,
            credentials_required=credentials_required,
            services=services,
            required_environment=required_environment,
            required_artifacts=required_artifacts,
            steps=steps,
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report

    @staticmethod
    def _steps(*, profile: str, row_count: int, include_vendor_live: bool) -> tuple[LiveCertificationStep, ...]:
        if profile == "vendor_live" and include_vendor_live:
            return _vendor_steps(row_count=row_count)
        if profile == "type_matrix_certification":
            return _type_matrix_steps(row_count=row_count)
        if profile == "native_transfer":
            return native_transfer_steps(
                row_count=row_count,
                start_services_command=start_local_services_command(),
                prepare_mssql_command=prepare_local_mssql_database_command(),
                service_markers_command=local_service_markers_command(profile="native_transfer"),
            )
        if profile == "real_local":
            return _real_local_steps(row_count=row_count)
        return _local_steps(row_count=row_count)


def _local_steps(*, row_count: int) -> tuple[LiveCertificationStep, ...]:
    return _local_steps_for_mode(row_count=row_count, profile="local_live")


def _real_local_steps(*, row_count: int) -> tuple[LiveCertificationStep, ...]:
    return _local_steps_for_mode(row_count=row_count, profile="real_local")


def _type_matrix_steps(*, row_count: int) -> tuple[LiveCertificationStep, ...]:
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
            start_local_services_command(),
            tuple(),
        ),
        LiveCertificationStep(
            "prepare_local_mssql_database",
            prepare_local_mssql_database_command(),
            tuple(),
        ),
        LiveCertificationStep(
            "run_local_service_markers",
            local_service_markers_command(profile="type_matrix_certification"),
            ("local_service_markers_junit.xml", "service_markers.json"),
        ),
        LiveCertificationStep(
            "run_type_matrix_contracts",
            "uv run pytest -m type_matrix_certification tests/test_type_matrix_certification.py -q",
            ("type_matrix_contracts_junit.xml",),
        ),
        LiveCertificationStep(
            "run_type_matrix_live_fixtures",
            "uv run pytest tests/integration/mssql/test_mssql_to_clickhouse_native_transfer_integration.py "
            "tests/integration/mssql/test_postgres_to_mssql_native_transfer_integration.py -q",
            ("live_fixture_junit.xml",),
        ),
        LiveCertificationStep(
            "build_type_matrix_planning_artifacts",
            "Build type_matrix_decisions.json, physical_ddl_plan.sql, schema_evolution_rerun.json, "
            "and live_fixture_summary.md without asserting route certification.",
            (
                "type_matrix_decisions.json",
                "physical_ddl_plan.sql",
                "schema_evolution_rerun.json",
                "live_fixture_summary.md",
            ),
        ),
        LiveCertificationStep(
            "stop_local_services",
            "docker compose -f docker/docker-compose.integration.yml down -v",
            tuple(),
            required=False,
        ),
    )


def _local_steps_for_mode(*, row_count: int, profile: str) -> tuple[LiveCertificationStep, ...]:
    matrix_env = (
        "DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_MATRIX=1 "
        "DPONE_MATRIX_RUN_MODE=mock_contract "
        f"DPONE_MATRIX_ROW_COUNT={row_count} "
        "DPONE_MATRIX_ARTIFACT_DIR=test_artifacts/live_certification/matrix"
    )
    return (
        LiveCertificationStep(
            "install_native_tooling",
            "Install unixODBC, Microsoft ODBC Driver 18, mssql-tools18/bcp, clickhouse-client, and docker compose.",
            tuple(),
            required=False,
        ),
        LiveCertificationStep(
            "start_local_services",
            start_local_services_command(),
            tuple(),
        ),
        LiveCertificationStep(
            "prepare_local_mssql_database",
            prepare_local_mssql_database_command(),
            tuple(),
        ),
        LiveCertificationStep(
            "run_local_service_markers",
            local_service_markers_command(profile=profile),
            ("local_service_markers_junit.xml", "service_markers.json"),
        ),
        LiveCertificationStep(
            "run_mysql_local_route_cells",
            _mysql_route_command(profile=profile),
            (_MYSQL_ROUTE_JUNIT.name, _MYSQL_ROUTE_EVIDENCE.name),
        ),
        LiveCertificationStep(
            "run_source_sink_matrix",
            f"{matrix_env} uv run pytest -m integration_matrix tests/integration/matrix -q "
            "--junitxml=test_artifacts/live_certification/matrix/junit.xml",
            ("junit.xml",),
        ),
        LiveCertificationStep(
            "build_matrix_report",
            "uv run dpone ops integration-matrix-report --artifact-dir test_artifacts/live_certification/matrix --output-dir test_artifacts/live_certification/matrix --format json",
            ("certification_report.json", "certification_report.md"),
        ),
        LiveCertificationStep(
            "stop_local_services",
            "docker compose -f docker/docker-compose.integration.yml down -v",
            tuple(),
            required=False,
        ),
    )


def _mysql_route_command(*, profile: str) -> str:
    selected_cells = " ".join(
        f"tests/integration/mysql/test_mysql_to_{sink}_native_transfer_integration.py::test_mysql_to_{sink}_{case}"
        for sink, cases in _MYSQL_LOCAL_ROUTE_CELLS.items()
        for case in cases
    )
    return (
        f"DPONE_RUN_INTEGRATION=1 uv run pytest {selected_cells} -q --junitxml={_MYSQL_ROUTE_JUNIT} "
        "&& uv run python tools/ci/assert_junit_executed.py "
        f"--junit {_MYSQL_ROUTE_JUNIT} --min-passed 5 --max-skipped 0 "
        f"--evidence-json {_MYSQL_ROUTE_EVIDENCE} --profile {profile} "
        '--commit-sha "$(git rev-parse HEAD)"'
    )


def _vendor_steps(*, row_count: int) -> tuple[LiveCertificationStep, ...]:
    required_env_args = " ".join(f"--required-env {name}" for name in DEFAULT_VENDOR_LIVE_ENV)
    matrix_env = (
        "DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_MATRIX=1 "
        "DPONE_MATRIX_RUN_MODE=vendor_live DPONE_MATRIX_STRATEGY=backfill "
        f"DPONE_MATRIX_ROW_COUNT={row_count} "
        "DPONE_MATRIX_ARTIFACT_DIR=test_artifacts/live_certification_vendor/backfill_matrix"
    )
    return (
        LiveCertificationStep(
            "verify_vendor_live_secrets",
            "Verify vendor_live_secrets are configured in CI secret manager.",
            tuple(),
        ),
        LiveCertificationStep(
            "verify_managed_credentials_readiness",
            "uv run dpone ops managed-credentials-readiness "
            "--profile vendor_live "
            f"{required_env_args} "
            "--output-dir test_artifacts/live_certification_vendor/managed-credentials "
            "--format json",
            ("managed_credentials_readiness.json", "managed_credentials_readiness.md"),
        ),
        LiveCertificationStep(
            "run_vendor_live_backfill_matrix",
            f"{matrix_env} uv run pytest -m integration_matrix tests/integration/matrix -q "
            "--junitxml=test_artifacts/live_certification_vendor/backfill_matrix/backfill_vendor_live_junit.xml",
            ("backfill_vendor_live_junit.xml",),
        ),
        LiveCertificationStep(
            "run_vendor_live_tests",
            f"DPONE_RUN_INTEGRATION_LIVE=1 DPONE_MATRIX_ROW_COUNT={row_count} "
            "uv run pytest -m integration_live tests/integration -q "
            "--junitxml=test_artifacts/live_certification_vendor/vendor_live_junit.xml && "
            "uv run python tools/ci/assert_junit_executed.py "
            "--junit test_artifacts/live_certification_vendor/vendor_live_junit.xml "
            "--min-passed 1 --max-skipped 0 --evidence-json "
            "test_artifacts/live_certification_vendor/vendor_live_tests.json "
            '--profile vendor_live --commit-sha "$(git rev-parse HEAD)"',
            ("vendor_live_junit.xml", "vendor_live_tests.json"),
        ),
    )
