from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_cicd_docs_cover_public_workflows_and_default_branch() -> None:
    text = _read("docs/ci-cd.md")
    for workflow in (
        ".github/workflows/ci.yml",
        ".github/workflows/pages.yml",
        ".github/workflows/release.yml",
        ".github/workflows/integration-matrix.yml",
        ".github/workflows/full-certification.yml",
        ".github/workflows/observability-maturity.yml",
        ".github/workflows/production-maturity.yml",
        ".github/workflows/connector-certification.yml",
        ".github/workflows/secret-scan.yml",
        ".github/workflows/codeql.yml",
        ".github/workflows/scorecard.yml",
        ".github/workflows/oss-code-quality-benchmark.yml",
    ):
        assert workflow in text
    assert "default branch for the public release flow is `master`" in text


def test_oss_benchmark_workflow_publishes_trust_center_artifacts() -> None:
    workflow = _read(".github/workflows/oss-code-quality-benchmark.yml")
    docs = _read("docs/ci-cd.md")

    assert "docs/benchmarks/" in workflow
    assert "test_artifacts/oss-code-quality-benchmark/" in workflow
    for expected in (
        "dpone-trust-center-snapshot-2026-06-12.md",
        "customer trust-center snapshot",
        "JSON and badge SVG",
    ):
        assert expected in docs


def test_cicd_detailed_docs_and_runbooks_are_linked_from_index() -> None:
    index = _read("docs/ci-cd.md")
    for path in (
        "cicd/workflows.md",
        "cicd/runbooks.md",
        "cicd/release-and-pages.md",
        "developer-ci-cd.md",
        "testing/manual-integration-matrix.md",
        "connector-certification.md",
    ):
        assert path in index
        assert (ROOT / "docs" / path).exists()


def test_cicd_runbooks_cover_every_workflow_family() -> None:
    runbooks = _read("docs/cicd/runbooks.md")
    for heading in (
        "CI quality failures",
        "PostgreSQL XMin integration failures",
        "Docs and GitHub Pages failures",
        "Release and PyPI failures",
        "Secret scan failures",
        "CodeQL failures",
        "OSSF Scorecard failures",
        "Source sink integration matrix failures",
        "Connector certification failures",
    ):
        assert heading in runbooks


def test_developer_docs_include_cicd_development_guidance() -> None:
    developers = _read("docs/developers.md")
    workflow = _read("docs/developer-workflow.md")
    guide = _read("docs/developer-ci-cd.md")
    mkdocs = _read("mkdocs.yml")

    assert "developer-ci-cd.md" in developers
    assert "developer-ci-cd.md" in workflow
    assert "Developer CI/CD guide: developer-ci-cd.md" in mkdocs
    for required in (
        "Keep the default PR gate deterministic and credential-free",
        "New workflow checklist",
        "Adding a manual integration gate",
        "PR description template for CI/CD changes",
    ):
        assert required in guide


def test_docs_do_not_reintroduce_stale_public_release_references() -> None:
    searchable_docs = [ROOT / "README.md", *sorted((ROOT / "docs").rglob("*.md"))]
    stale_patterns = (
        "branches: [main]",
        "pull requests to `main`",
        "pushes to `main`",
        "origin/main",
        "refs/heads/main",
        "v0.2.1",
        "dpone[full]==0.2.1",
        "next implementation step is a live CDC reader",
        "MSSQL Change Tracking contract placeholder",
        "data-platform-dpone",
        "dltaf",
        "example_travel",
        "GitLab Package Registry",
    )
    offenders: list[str] = []
    for path in searchable_docs:
        text = path.read_text(encoding="utf-8")
        for pattern in stale_patterns:
            if pattern in text:
                offenders.append(f"{path.relative_to(ROOT)}: {pattern}")
    assert offenders == []


def test_testing_docs_are_consolidated_under_testing_folder() -> None:
    root_testing_docs = (
        ROOT / "docs" / "TESTING.md",
        ROOT / "docs" / "INTEGRATION_TESTS.md",
        ROOT / "docs" / "INTEGRATION_MATRIX.md",
    )
    assert [path for path in root_testing_docs if path.exists()] == []

    index = _read("docs/testing/index.md")
    for path in (
        "overview.md",
        "integration-tests.md",
        "manual-integration-matrix.md",
        "integration-matrix.md",
        "local-mssql-mock-matrix.md",
    ):
        assert path in index
        assert (ROOT / "docs" / "testing" / path).exists()


def test_manual_workflows_publish_strategy_certification_bundles() -> None:
    workflow_expectations = {
        ".github/workflows/replay-integration.yml": (
            "Build replay strategy certification bundle",
            "--replay-evidence",
            "docs/testing/replay-integration.md",
            "strategy-certification-replay",
        ),
        ".github/workflows/integration-matrix.yml": (
            "Build matrix strategy certification bundle",
            "--matrix-artifact",
            "docs/testing/integration-matrix.md",
            "strategy-certification-matrix",
        ),
        ".github/workflows/connector-certification.yml": (
            "Build connector strategy certification bundle",
            "--connector-artifact",
            "docs/connector-certification.md",
            "strategy-certification-connectors",
        ),
    }

    for workflow, expected_parts in workflow_expectations.items():
        text = _read(workflow)
        assert "uv run dpone strategy certification-bundle" in text
        assert "test_artifacts/strategy_certification" in text
        for expected in expected_parts:
            assert expected in text


def test_manual_workflows_publish_final_certification_suites() -> None:
    matrix = _read(".github/workflows/integration-matrix.yml")
    connectors = _read(".github/workflows/connector-certification.yml")

    assert "uv run dpone ops integration-matrix-report" in matrix
    assert "uv run dpone ops certification-suite" in matrix
    assert (
        "--strategy-certification-bundle test_artifacts/strategy_certification/matrix/strategy_certification_bundle.json"
        in matrix
    )
    assert "source-sink-certification-suite" in matrix

    assert "uv run dpone ops certification-suite" in connectors
    assert (
        "--strategy-certification-bundle test_artifacts/strategy_certification/connectors/strategy_certification_bundle.json"
        in connectors
    )
    assert "connector-certification-suite" in connectors


def test_manual_workflows_publish_tamper_evident_evidence_chains() -> None:
    replay = _read(".github/workflows/replay-integration.yml")
    matrix = _read(".github/workflows/integration-matrix.yml")
    connectors = _read(".github/workflows/connector-certification.yml")

    for workflow_text, artifact_name in (
        (replay, "replay-evidence-chain"),
        (matrix, "source-sink-evidence-chain"),
        (connectors, "connector-evidence-chain"),
    ):
        assert "uv run dpone ops artifact-index" in workflow_text
        assert "uv run dpone ops evidence-chain" in workflow_text
        assert "uv run dpone ops evidence-chain-verify" in workflow_text
        assert "artifact_index.json" in workflow_text
        assert artifact_name in workflow_text

    docs = _read("docs/cicd/workflows.md")
    assert "tamper-evident evidence chain" in docs
    assert "evidence-chain-verify" in docs
    assert "replay-evidence-chain" in docs
    assert "source-sink-evidence-chain" in docs
    assert "connector-evidence-chain" in docs


def test_release_summary_workflow_downloads_evidence_and_publishes_go_no_go_report() -> None:
    workflow = _read(".github/workflows/certification-release-summary.yml")
    docs = _read("docs/cicd/workflows.md")

    for expected in (
        "replay_run_id",
        "matrix_run_id",
        "connector_run_id",
        "gh run download",
        "uv run dpone ops release-summary",
        "release-summary-report",
    ):
        assert expected in workflow
    assert "release-summary-report" in docs
    assert "dpone ops release-summary" in docs


def test_orchestration_maturity_workflow_runs_focused_gate_and_is_documented() -> None:
    workflow = _read(".github/workflows/orchestration-maturity.yml")
    docs = _read("docs/cicd/workflows.md")
    runbooks = _read("docs/cicd/runbooks.md")

    for expected in (
        "uv run pytest tests/test_orchestration.py -q",
        "uv run dpone docs check-docs",
        "uv run mkdocs build --strict",
        "orchestration-maturity-report",
    ):
        assert expected in workflow
    assert ".github/workflows/orchestration-maturity.yml" in docs
    assert "orchestration-maturity-report" in docs
    assert "Orchestration maturity failures" in runbooks


def test_primary_ci_enforces_governed_rest_delivery_design_contract() -> None:
    workflow = _read(".github/workflows/ci.yml")

    assert "uv run dpone docs check-rest-delivery-design-contract" in workflow
    assert "--format json" in workflow
    assert "Smoke REST design contract from clean base wheel" in workflow
    assert "uv pip install --python /tmp/dpone-rest-contract-smoke/bin/python --find-links dist" in workflow
    assert "/tmp/dpone-rest-contract-smoke/bin/dpone docs check-rest-delivery-design-contract" in workflow


def test_full_certification_workflow_runs_matrix_benchmark_lineage_and_evidence() -> None:
    workflow = _read(".github/workflows/full-certification.yml")

    for expected in (
        "schedule:",
        "uv run dpone ops certification-automation-plan",
        "uv run pytest -m integration_matrix tests/integration/matrix -q",
        "uv run dpone ops integration-matrix-report",
        "uv run dpone ops benchmark-baseline",
        "uv run dpone ops run-registry",
        "uv run dpone ops lineage-export",
        "uv run dpone ops evidence-bundle",
        "uv run dpone strategy certification-bundle",
        "uv run dpone ops certification-suite",
        "--require-benchmark",
        "--require-lineage",
        "--require-evidence",
        "--require-strategy-certification",
        "uv run dpone ops evidence-chain-verify",
        "full-certification-report",
    ):
        assert expected in workflow


def test_full_certification_workflow_is_documented_in_user_and_runbook_docs() -> None:
    index = _read("docs/ci-cd.md")
    workflows = _read("docs/cicd/workflows.md")
    runbooks = _read("docs/cicd/runbooks.md")
    suite = _read("docs/certification-suite.md")
    ops = _read("docs/ops-cli.md")

    for text in (index, workflows, runbooks, suite, ops):
        assert "full-certification.yml" in text
    assert "Full certification automation failures" in runbooks
    assert "dpone ops certification-automation-plan" in ops


def test_observability_maturity_workflow_exports_metrics_and_is_documented() -> None:
    workflow = _read(".github/workflows/observability-maturity.yml")
    index = _read("docs/ci-cd.md")
    workflows = _read("docs/cicd/workflows.md")
    runbooks = _read("docs/cicd/runbooks.md")
    user_doc = _read("docs/observability.md")
    developer_doc = _read("docs/developer-observability.md")

    for expected in (
        "uv run pytest tests/test_observability.py -q",
        "uv run dpone observability metrics-export",
        "--resource-attr deployment.environment=ci",
        "observability-maturity-report",
    ):
        assert expected in workflow
    for text in (index, workflows, runbooks, user_doc, developer_doc):
        assert "observability-maturity.yml" in text
    assert "metrics_index.json" in user_doc
    assert "Observability maturity failures" in runbooks


def test_production_maturity_workflow_aggregates_ga_evidence_and_is_documented() -> None:
    workflow = _read(".github/workflows/production-maturity.yml")
    index = _read("docs/ci-cd.md")
    workflows = _read("docs/cicd/workflows.md")
    runbooks = _read("docs/cicd/runbooks.md")
    ops = _read("docs/ops-cli.md")
    maturity = _read("docs/production-maturity.md")

    for expected in (
        "uv run dpone ops production-maturity",
        "--artifact cdc=",
        "--artifact supply_chain=",
        "production-maturity-report",
    ):
        assert expected in workflow
    for text in (index, workflows, runbooks, ops, maturity):
        assert "production-maturity.yml" in text
        assert "dpone ops production-maturity" in text
    assert "CDC, performance, security, supply-chain" in maturity
    assert "does not grant" in maturity


def test_strategy_certification_bundle_is_documented_in_cicd_reference() -> None:
    workflows = _read("docs/cicd/workflows.md")
    suite = _read("docs/certification-suite.md")

    for required in (
        "strategy_certification_bundle.json",
        "strategy_certification_bundle.md",
        "dpone strategy certification-bundle",
    ):
        assert required in workflows
        assert required in suite


def test_oss_code_quality_benchmark_workflow_is_manual_and_self_service() -> None:
    workflow = _read(".github/workflows/oss-code-quality-benchmark.yml")
    index = _read("docs/ci-cd.md")

    for expected in (
        "workflow_dispatch:",
        "project:",
        "allow_stale:",
        "max_stale_days:",
        "create_pr:",
        "verify_source_urls:",
        '--runner-name "${{ github.actor }}"',
        '--run-id "${{ github.run_id }}"',
        '--run-url "${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}"',
        '--git-sha "${{ github.sha }}"',
        '--branch "${{ github.ref_name }}"',
        '--release-version "$release_version"',
        '--release-tag "$release_tag"',
        '--release-sha "$release_sha"',
        "--verify-source-urls",
        "--baseline-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json",
        "--external-analyzer-timeout",
        "--enforce-quality-gates",
        "--run-certification",
        "--certification-mode local",
        "--fail-on-certification",
        "--pr-summary test_artifacts/oss-code-quality-benchmark/pr-comment.md",
        "Install external analyzer toolchain",
        "cargo install tokei",
        "uv pip install radon lizard",
        "cloc",
        "sling",
        "Verify public benchmark redaction",
        "actions/upload-artifact",
        "oss-code-quality-benchmark",
        "oss-release-readiness-seal.svg",
        "oss-benchmark-release-readiness-2026-06-12.md",
        "oss-benchmark-release-readiness-2026-06-12.json",
        "docs/benchmarks/data/runtime-certification/",
        "gh pr create",
        "--body-file test_artifacts/oss-code-quality-benchmark/pr-comment.md",
    ):
        assert expected in workflow
    assert ".github/workflows/oss-code-quality-benchmark.yml" in index
    assert "OSS code quality benchmark" in index
    assert "quality gates" in index
    assert "PR summary" in index
    assert "project=all, dpone, airbyte, dlt, pentaho-kettle, apache-hop, or sling" in index
    assert "oss-code-quality-benchmark-history.json" in index
    assert "oss-architecture-delta.svg" in index
    assert "oss-complexity-boundary.svg" in index
    assert "Complexity & Boundary Discipline" in index
    assert "maintainability risk register" in index
    assert "oss-refactor-roi-roadmap.svg" in index
    assert "Refactor ROI Roadmap" in index
    assert "Executable Certification" in index
    assert "runtime-certification/latest/run-ledger.json" in index
    assert "quality debt" in index
    assert "PR regression gate" in index
    assert "oss-evidence-confidence.svg" in index
    assert "oss-benchmark-provenance.json" in index
    assert "Evidence Trust & Auditability" in index
    assert "Source Citation Verification" in index
    assert "claim-to-source matrix" in index
    assert "oss-source-verification.svg" in index
    assert "Benchmark v3 Release Readiness" in index
    assert "Claims Ledger" in index
    assert "Runtime Certification Matrix" in index
    assert "Quality Budget As Code" in index
    assert "release readiness pack" in index
    assert "oss-release-readiness-seal.svg" in index

    workflows_doc = _read("docs/cicd/workflows.md")
    assert "verify_source_urls" in workflows_doc
    assert "Source Citation Verification" in workflows_doc
    assert "claim-to-source matrix" in workflows_doc
    assert "stale source values" in workflows_doc
    assert "Benchmark v3 Release Readiness" in workflows_doc
    assert "release readiness pack" in workflows_doc
    assert "External analyzer execution" in index
    assert "stale analyzer values" in index
    assert "public artifact redaction" in index
    assert "claim coverage" in index
    assert "oss-public-evidence-integrity.svg" in index
    assert "reproducibility manifest" in index
    assert "optional LOC/SLOC cross-check" in index
    assert "Semantic Maintainability Deep Scan" in index
    assert "oss-semantic-maintainability.svg" in index
    assert "oss-god-object-radar.svg" in index
    assert "SOLID/DI/Clean Code evidence" in index
    assert "DRY/KISS responsibility signals" in index
    assert "Scoring Validity & Calibration" in index
    assert "oss-score-calibration.svg" in index
    assert "oss-score-sensitivity.svg" in index
    assert "oss-normalized-vs-raw.svg" in index
    assert "Language/repo normalization" in index
    assert "Sensitivity analysis" in index
    assert "Anti-gaming guardrails" in index
    assert "Scale Readiness & Growth Simulation" in index
    assert "oss-scale-readiness.svg" in index
    assert "oss-architecture-runway.svg" in index
    assert "oss-quality-headroom.svg" in index
    assert "quality headroom" in index
    assert "architecture runway" in index
    assert "Independent Analyzer Cross-Validation" in index
    assert "oss-independent-validation.svg" in index
    assert "oss-analyzer-confidence.svg" in index
    assert "Analyzer command ledger" in index
    assert "external analyzer" in index
