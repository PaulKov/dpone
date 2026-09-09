"""Contract tests for Phase D route-live-wide-certification workflow."""

from __future__ import annotations

from pathlib import Path

import yaml
from tools.route_live_certification.contract import REQUIRED_SUITE_IDS

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "route-live-wide-certification.yml"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release-candidate-evidence.yml"
SEMANTIC_GATE = ROOT / "tools" / "validate_route_live_certification.py"
INVENTORY_WRITER = ROOT / "tools" / "route_live_certification" / "inventory_writer.py"
HELPER = ROOT / "tools" / "ci" / "assert_junit_executed.py"
PLANNER = ROOT / "tools" / "ci" / "plan_route_live_wide_matrix.py"
PORT_EXPORTER = ROOT / "tools" / "ci" / "export_route_live_compose_ports.py"
PYTEST_PROGRESS = ROOT / "tools" / "route_live_certification" / "pytest_plugin.py"
PROCESS_TREE_SUPERVISOR = ROOT / "tools" / "ci" / "run_bounded_process_tree.py"
DOCS = ROOT / "docs" / "testing" / "route-live-wide-certification.md"
SPEC = ROOT / "docs" / "feature-design-route-live-wide-ci-v1.md"
XMIN_MSSQL_LIVE_SUPPORT = ROOT / "tests/integration/postgres/postgres_xmin_mssql_snapshot_live_support.py"
XMIN_MSSQL_LIVE_TEST = ROOT / "tests/integration/postgres/test_postgres_xmin_mssql_snapshot_reconciliation_live.py"
XMIN_MSSQL_LIVE_MATRIX = ROOT / "tests/integration/postgres/postgres_xmin_mssql_snapshot_live_matrix.py"
COMPOSE = ROOT / "docker" / "docker-compose.integration.yml"


def _load_workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _triggers(workflow: dict) -> dict:
    return workflow.get("on") or workflow.get(True) or {}


def test_route_live_wide_workflow_triggers_are_gated() -> None:
    workflow = _load_workflow()
    triggers = _triggers(workflow)
    assert "workflow_dispatch" in triggers
    assert "workflow_call" in triggers
    assert "schedule" in triggers
    assert "pull_request" not in triggers
    assert "push" not in triggers

    inputs = triggers["workflow_dispatch"]["inputs"]
    assert set(inputs) >= {"source", "sink", "include_bigquery"}
    assert inputs["include_bigquery"]["default"] is False
    assert "bigquery" in inputs["sink"]["options"]
    assert "postgres" in inputs["source"]["options"]
    assert "mysql" in inputs["source"]["options"]
    assert "mssql" in inputs["source"]["options"]

    schedule = triggers["schedule"]
    assert isinstance(schedule, list) and schedule
    assert "cron" in schedule[0]


def test_route_live_manual_and_nightly_workflow_keeps_the_complete_suite_contract() -> None:
    assert REQUIRED_SUITE_IDS == (
        "artifact_integrity_faults",
        "backfill_orchestration",
        "boundary_types",
        "explicit_types",
        "lineage_parity",
        "physical_design",
        "postgis_types",
        "schema_evolution",
        "source_identity_authority",
        "strategy_capability",
        "target_behavior",
        "target_database_authority",
        "target_identity_authority",
        "text_key_lifecycle",
        "transaction_governance",
        "wide_performance_soak",
        "wide_strategy",
        "xmin_reconciliation",
    )


def test_route_live_wide_workflow_enforces_skip_ne_pass_policy() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    workflow = _load_workflow()

    assert "pull_request" not in text.split("on:", 1)[1].split("jobs:", 1)[0]
    assert "assert_junit_executed.py" in text
    assert "plan_route_live_wide_matrix.py" in text
    assert "SKIP≠PASS" in text or "SKIP!=PASS" in text or "SKIP ≠ PASS" in text
    assert "BIGQUERY_DWH_PROJECT_ID" in text
    assert "BIGQUERY_DWH_SERVICE_ACCOUNT_JSON" in text
    assert "test_postgres_xmin_mssql_snapshot_reconciliation_live.py" in text
    assert text.count("test_postgres_mssql_artifact_integrity_faults_live.py") == 1
    assert text.count("test_postgres_mssql_backfill_orchestration_live.py") == 1
    assert text.count("test_postgres_mssql_lineage_parity_live.py") == 1
    assert "test_postgres_mssql_physical_design_matrix_live.py" in text
    assert "test_postgres_mssql_schema_evolution_matrix_live.py" in text
    assert text.count("test_postgres_mssql_source_identity_authority_live.py") == 1
    assert "test_postgres_mssql_strategy_capability_matrix_live.py" in text
    assert "test_postgres_mssql_text_key_lifecycle_live.py" in text
    assert "test_postgres_mssql_production_hydration_live.py" in text
    assert "test_postgres_mssql_transaction_governance_matrix_live.py" in text
    assert text.count("test_postgres_mssql_target_database_authority_live.py") == 1
    assert text.count("test_postgres_mssql_target_behavior_live.py") == 1
    assert text.count("test_postgres_mssql_wide_performance_soak_live.py") == 1
    assert "test_postgres_xmin_mssql_identity_authority_live.py" in text
    assert "--max-skipped 0" in text
    assert "validate_route_live_certification.py" in text
    assert "tools.route_live_certification.inventory_writer" in text
    assert "DPONE_ROUTE_LIVE_INVENTORY" in text
    assert "DPONE_ROUTE_LIVE_VENDOR_METADATA" in text
    assert '"$root/inventory.json"' in text
    assert "postgres_mssql_route_live_inventory.v1.json" not in text
    assert "DPONE_ROUTE_LIVE_EVIDENCE_ROOT" in text
    assert "DPONE_ROUTE_LIVE_JUNIT_ROOT" in text
    assert "python -m json.tool" not in text
    assert "certification_passed" not in text  # semantic gate owns the accepted status.
    assert "if-no-files-found: error" in text

    plan = workflow["jobs"]["plan-matrix"]
    assert "docker_matrix" in plan["outputs"]
    assert "run_bq" in plan["outputs"]

    docker = workflow["jobs"]["docker-route-live-wide"]
    assert docker["needs"] == ["plan-matrix"]
    assert "run_docker" in docker["if"]
    assert "matrix.source" not in docker["if"]
    assert "fromJSON(needs.plan-matrix.outputs.docker_matrix)" in str(docker["strategy"]["matrix"])
    for name in (
        "MATRIX_UPLOAD_ROOT",
        "ROUTE_CERTIFICATION_ROOT",
        "DPONE_ROUTE_LIVE_EVIDENCE_ROOT",
        "DPONE_ROUTE_LIVE_JUNIT_ROOT",
    ):
        assert name not in docker["env"]

    bq = workflow["jobs"]["bigquery-route-live-wide"]
    assert bq["needs"] == ["plan-matrix"]
    assert "run_bq" in bq["if"]
    assert "matrix.source" not in bq["if"]
    assert "fromJSON(needs.plan-matrix.outputs.bq_matrix)" in str(bq["strategy"]["matrix"])

    step_names = [step.get("name", "") for step in docker["steps"]]
    assert "Bind route-live temporary roots" in step_names
    assert "Preflight integration env" in step_names
    assert "Capture reviewed PostgreSQL to MSSQL inventory" in step_names
    assert "Assert junit executed (SKIP≠PASS)" in step_names
    assert "Consolidate strict PostgreSQL to MSSQL release evidence" in step_names
    assert "Assemble single-root matrix artifact" in step_names
    upload = next(step for step in docker["steps"] if step.get("name") == "Upload route live wide artifacts")
    assert upload["with"]["path"] == "${{ runner.temp }}/dpone-route-live-upload/"
    assert "\n" not in upload["with"]["path"]
    assembly = next(step for step in docker["steps"] if step.get("name") == "Assemble single-root matrix artifact")
    assert "$MATRIX_UPLOAD_ROOT/dpone-route-live-certification" in assembly["run"]
    assert "$MATRIX_UPLOAD_ROOT/diagnostics" in assembly["run"]
    temp_binding = next(step for step in docker["steps"] if step.get("name") == "Bind route-live temporary roots")
    assert "${RUNNER_TEMP}/dpone-route-live-certification" in temp_binding["run"]
    assert temp_binding["run"].count('>> "$GITHUB_ENV"') == 4
    assert HELPER.is_file()
    assert PLANNER.is_file()
    assert PORT_EXPORTER.is_file()
    assert PYTEST_PROGRESS.is_file()
    assert PROCESS_TREE_SUPERVISOR.is_file()
    assert SEMANTIC_GATE.is_file()
    assert INVENTORY_WRITER.is_file()


def test_route_live_docker_suite_is_observable_and_bounded_below_job_timeout() -> None:
    workflow = _load_workflow()
    docker = workflow["jobs"]["docker-route-live-wide"]
    assert docker["timeout-minutes"] == 60

    run_step = next(step for step in docker["steps"] if step.get("name") == "Run route wide vendor-live suite")
    run = run_step["run"]
    assert 'DPONE_ROUTE_LIVE_PROGRESS_JSONL="$ARTIFACT_ROOT/pytest-progress.jsonl"' in run
    assert "tools/ci/run_bounded_process_tree.py" in run
    assert "--timeout-seconds 2700" in run
    assert "--term-grace-seconds 30" in run
    assert "--kill-grace-seconds 10" in run
    assert '-- .venv/bin/python -m pytest "${modules[@]}" -vv -rs --maxfail=1 --durations=25' in run
    assert "timeout --signal=TERM --kill-after=30s 45m" not in run
    assert "-o faulthandler_timeout=60" in run
    assert '"$ARTIFACT_ROOT/pytest-exit.json"' in run
    assert '"diagnostic_only":true' in run
    assert '"release_ready":false' in run
    assert '"commit_sha":"%s"' in run
    assert '"$GITHUB_SHA" "$GITHUB_RUN_ID" "$GITHUB_RUN_ATTEMPT"' in run

    junit_step = next(step for step in docker["steps"] if step.get("name") == "Assert junit executed (SKIP≠PASS)")
    assert junit_step["if"] == "always()"
    assert '--evidence-json "$ARTIFACT_ROOT/junit-gate.json"' in junit_step["run"]
    assert '--summary-md "$GITHUB_STEP_SUMMARY"' in junit_step["run"]

    cleanup = next(step for step in docker["steps"] if step.get("name") == "Stop disposable local services")
    assert cleanup["if"] == "always()"
    assert "timeout --signal=TERM --kill-after=15s 180s" in cleanup["run"]
    assert "down -v --timeout 30" in cleanup["run"]


def test_route_live_docker_services_use_daemon_allocated_loopback_ports() -> None:
    workflow = _load_workflow()
    docker = workflow["jobs"]["docker-route-live-wide"]
    start = next(step for step in docker["steps"] if step.get("name") == "Start disposable local services")
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))

    bindings = {
        "postgres": ("DPONE_IT_PG_PORT_FORWARD", 55432, 5432),
        "postgis": ("DPONE_IT_POSTGIS_PORT_FORWARD", 55433, 5432),
        "postgres-authority-primary": ("DPONE_IT_PG_AUTHORITY_PRIMARY_PORT_FORWARD", 55434, 5432),
        "postgres-authority-standby": ("DPONE_IT_PG_AUTHORITY_STANDBY_PORT_FORWARD", 55435, 5432),
        "mssql": ("DPONE_IT_MSSQL_PORT_FORWARD", 51433, 1433),
    }
    assert start["env"] == {variable: "127.0.0.1:" for variable, _, _ in bindings.values()}
    for service, (variable, default_port, container_port) in bindings.items():
        port_template = compose["services"][service]["ports"][0]
        placeholder = f"${{{variable}:-{default_port}}}"
        assert port_template == f"{placeholder}:{container_port}"
        rendered = port_template.replace(placeholder, start["env"][variable])
        assert rendered == f"127.0.0.1::{container_port}"

    run = start["run"]
    assert "export_route_live_compose_ports.py" in run
    assert '--github-env "$GITHUB_ENV"' in run
    assert "docker compose -f docker/docker-compose.integration.yml up -d --wait" in run
    assert run.index("up -d --wait") < run.index("export_route_live_compose_ports.py")

    workflow_text = WORKFLOW.read_text(encoding="utf-8")
    for fixed_port in ("55432", "55433", "55434", "55435", "51433"):
        assert fixed_port not in workflow_text


def test_release_candidate_keeps_wide_route_authority_outside_the_tag_gate() -> None:
    release_text = RELEASE_WORKFLOW.read_text(encoding="utf-8")

    assert "collect-postgres-mssql-route-evidence" not in release_text
    assert "uses: ./.github/workflows/route-live-wide-certification.yml" not in release_text
    assert "write_route_live_provider_binding.py" not in release_text
    assert "postgres_mssql_route_certification.json" not in release_text


def test_release_route_images_are_immutable_and_match_inventory_allowlist() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    vendors = (ROOT / "tools/route_live_certification/vendors.py").read_text(encoding="utf-8")
    digests = (
        "44c4ee9810eff91f7eab4d822642e01115b1a9eccce4bcbdde7604752d68eac6",
        "b193e996618e9e632e2c6e268462b350c28a9c871cb0352b32905fc01e0299bd",
        "ba4c8329f48fb8f02e1416be6a930ebfd71268caee78aa985f3af4315e457c89",
    )
    for digest in digests:
        assert digest in compose
        assert digest in vendors
    assert compose.count("mcr.microsoft.com/mssql/server:2022-latest@sha256:") == 2
    assert compose.count("image: postgres:16-alpine@sha256:") == 3
    assert compose.count("platform: linux/amd64") >= 3
    assert "image: postgres:16-alpine\n" not in compose
    assert "image: mcr.microsoft.com/mssql/server:2022-latest\n" not in compose


def test_source_identity_authority_topology_is_pinned_and_healthchecked() -> None:
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = compose["services"]
    primary = services["postgres-authority-primary"]
    standby = services["postgres-authority-standby"]

    assert primary["image"] == services["postgres"]["image"]
    assert standby["image"] == services["postgres"]["image"]
    assert "healthcheck" in primary and "healthcheck" in standby
    assert standby["depends_on"]["postgres-authority-primary"]["condition"] == "service_healthy"
    assert "pg_basebackup" in "\n".join(standby["entrypoint"])
    assert "--write-recovery-conf" in "\n".join(standby["entrypoint"])

    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "services+=(postgis postgres-authority-primary postgres-authority-standby)" in workflow
    assert "authority_primary_digest" in workflow
    assert "authority_standby_digest" in workflow


def test_xmin_route_live_test_is_isolated_from_vendor_api_certification() -> None:
    route_test = (
        ROOT / "tests/integration/postgres/test_postgres_xmin_mssql_snapshot_reconciliation_live.py"
    ).read_text(encoding="utf-8")
    generic = (ROOT / ".github/workflows/live-certification.yml").read_text(encoding="utf-8")

    assert "pytest.mark.route_live_wide" in route_test
    assert '-m "integration_live and not route_live_wide"' in generic


def test_xmin_route_live_state_preflight_uses_the_provisioned_run_table() -> None:
    support = XMIN_MSSQL_LIVE_SUPPORT.read_text(encoding="utf-8")

    checkpoint_storage = support.split("checkpoint_storage = MSSQLXMinStateStorage(", 1)[1].split(")\n", 1)[0]
    assert 'run_table="dpone_run_state"' in checkpoint_storage
    assert 'run_table="etl_run_state"' not in checkpoint_storage


def test_xmin_route_live_executes_required_real_vendor_matrix_without_release_claim() -> None:
    route_test = XMIN_MSSQL_LIVE_TEST.read_text(encoding="utf-8")
    matrix = XMIN_MSSQL_LIVE_MATRIX.read_text(encoding="utf-8")

    assert "run_vendor_live_matrix" in route_test
    assert "REQUIRED_LIVE_MATRIX_CASES" in route_test
    for scenario in (
        "parallel_applock_and_stale_cas",
        "concurrent_source_mutation_same_snapshot",
        "state_permission_denial",
        "per_dml_and_checkpoint_fault_injection",
        "empty_guard_and_checksum_negative_cases",
    ):
        assert scenario in matrix
    assert 'evidence["status"] = "passed_partial"' in matrix
    assert 'evidence["release_ready"] = False' in matrix


def test_route_live_wide_docs_and_spec_point_at_workflow() -> None:
    docs = DOCS.read_text(encoding="utf-8")
    spec = SPEC.read_text(encoding="utf-8")
    assert "route-live-wide-certification.yml" in docs
    assert "SKIP" in docs and "PASS" in docs
    assert "Status: APPROVED" in spec or "Status: IMPLEMENTED" in spec
    assert "route-live-wide-certification.yml" in spec
