from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "connector-certification.yml"
DOCS = ROOT / "docs"


def _workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_local_live_connector_certification_starts_declared_service_set() -> None:
    workflow = _workflow_text()

    assert "Install SQL Server ODBC and bcp tooling" in workflow
    assert "ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18" in workflow
    assert (
        "docker compose -f docker/docker-compose.integration.yml "
        "up -d postgres clickhouse minio mssql kafka schema-registry"
    ) in workflow
    assert "Prepare local MSSQL database" in workflow
    assert "MSSQL did not become ready for local connector certification." in workflow
    assert "CREATE DATABASE ' + QUOTENAME" in workflow
    assert "DPONE_IT_MSSQL_BCP_PATH: /opt/mssql-tools18/bin/bcp" in workflow
    assert "DPONE_KAFKA_BOOTSTRAP_SERVERS: 127.0.0.1:59092" in workflow
    assert "DPONE_SCHEMA_REGISTRY_URL: http://127.0.0.1:58081" in workflow


def test_vendor_live_connector_certification_is_scoped_to_provider_directories() -> None:
    workflow = _workflow_text()
    vendor_job = workflow.split("vendor-live-certification:", maxsplit=1)[1]

    assert "uv run pytest -m integration_live tests/integration -q" not in vendor_job
    for provider_path in (
        "tests/integration/appsflyer",
        "tests/integration/cbr",
        "tests/integration/fasttrack",
        "tests/integration/google_ads",
        "tests/integration/google_sheets",
        "tests/integration/mindbox",
        "tests/integration/openexchangerates",
        "tests/integration/similarweb",
        "tests/integration/yandex_webmaster",
    ):
        assert provider_path in vendor_job

    assert "tests/integration/mssql" not in vendor_job
    assert "tests/integration/clickhouse" not in vendor_job
    assert "tests/integration/kafka" not in vendor_job
    assert "tests/integration/matrix" not in vendor_job
    assert "--junitxml=test_artifacts/connectors/vendor-live/junit.xml" in vendor_job
    assert "Enforce vendor live junit policy" in vendor_job
    assert "Detect vendor secret readiness" in vendor_job
    assert "secret-readiness.json" in vendor_job
    # Strict zero-skip gate remains for manual dispatch only.
    assert "--max-skipped 0" in vendor_job
    assert "workflow_dispatch" in vendor_job
    assert "Schedule vendor-live gate: allow credential skips" in vendor_job
    assert "Upload vendor live execution evidence" in vendor_job
    assert "if: always()" in vendor_job


def test_connector_certification_workflow_documents_non_required_authority() -> None:
    workflow = _workflow_text()
    assert "NEVER a merge/release required check" in workflow
    assert "Schedule hygiene" in workflow or "schedule hygiene" in workflow.lower()


def test_connector_certification_docs_record_async_gate_and_scope_taxonomy() -> None:
    connector_docs = (DOCS / "connector-certification.md").read_text(encoding="utf-8")
    ci_docs = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    workflow_docs = (DOCS / "cicd" / "workflows.md").read_text(encoding="utf-8")
    runbook = (DOCS / "cicd" / "runbooks.md").read_text(encoding="utf-8")
    release_evidence = (DOCS / "release-evidence.md").read_text(encoding="utf-8")
    docs = "\n".join((connector_docs, ci_docs, workflow_docs, runbook, release_evidence))

    assert "async scheduled release-evidence gate" in docs
    assert "ODBC Driver 18" in docs
    assert "mssql-tools18" in docs
    assert "materializes the MSSQL test database" in docs
    assert "provider/API directories" in docs
    assert "must not run local Docker route tests" in docs
    assert "zero skipped tests" in docs
    assert "required status check" in connector_docs
    assert "schedule allows credential-gated skips" in connector_docs
    assert "NEVER a merge/release required check" in _workflow_text()
    assert "Record the workflow run ID, status, and artifacts" in docs
