from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.ops.approval_record import ApprovalRecordService
from dpone.ops.artifact_index import ArtifactIndexService
from dpone.ops.benchmark_baseline import BenchmarkBaselineService
from dpone.ops.certification import CertificationHarnessService
from dpone.ops.certification_artifacts import CertificationArtifactReader, production_artifact_payload_passed
from dpone.ops.certification_automation import CertificationAutomationPlanService
from dpone.ops.certification_history import CertificationHistoryService
from dpone.ops.certification_suite import CertificationSuiteService
from dpone.ops.change_request import ChangeRequestService
from dpone.ops.checksums import sha256_file
from dpone.ops.connector_badges import ConnectorBadgeService
from dpone.ops.contracts import DataContractService
from dpone.ops.dbt_lineage import DbtLineageService
from dpone.ops.deployment_record import DeploymentRecordService
from dpone.ops.diff import OpsDiffService
from dpone.ops.docs_publish_pack import DocsPublishPackService
from dpone.ops.env_drift import EnvironmentDriftService
from dpone.ops.evidence import EvidenceBundleService, EvidenceItem, OpsEvidenceBundle
from dpone.ops.evidence_chain import EvidenceChainService
from dpone.ops.gate import GoLiveGateService
from dpone.ops.incident import OpsIncidentPackService
from dpone.ops.industrial_readiness import IndustrialReadinessService
from dpone.ops.integration_matrix_report import IntegrationMatrixReportService
from dpone.ops.manifest_bundle import ManifestBundleService
from dpone.ops.marketplace import ConnectorCatalogEntry, ConnectorMarketplaceService
from dpone.ops.openlineage_export import OpenLineageExportService
from dpone.ops.packages import LoadPackageService, LoadPackageStatus
from dpone.ops.policy import OpsPolicyService
from dpone.ops.post_deploy_verify import PostDeployVerifyService
from dpone.ops.quarantine import QuarantineService
from dpone.ops.release_close import ReleaseCloseService
from dpone.ops.release_gate import ReleaseGateService
from dpone.ops.release_orchestrator import ReleaseOrchestratorService
from dpone.ops.release_promote import ReleasePromotionService
from dpone.ops.release_summary import ReleaseSummaryService
from dpone.ops.rollback import RollbackPlanService
from dpone.ops.rollback_execute import RollbackExecutionService
from dpone.ops.run_registry import RunRegistryService
from dpone.ops.runbook_pack import RunbookPackService
from dpone.ops.security import OpsSecurityAuditService
from dpone.ops.slo import OpsSloService
from dpone.readiness.capability_discovery_composition import (
    build_capability_discovery_service,
)


def _write_bound_artifact_index(
    path: Path,
    *,
    release: str,
    artifacts: tuple[Path, ...],
) -> Path:
    path.write_text(
        json.dumps(
            {
                "release": release,
                "passed": True,
                "items": [
                    {
                        "name": artifact.name,
                        "path": str(artifact),
                        "sha256": sha256_file(artifact),
                    }
                    for artifact in artifacts
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def test_legacy_operations_namespace_points_to_canonical_ops_package() -> None:
    from dpone.operations.certification import CertificationHarnessService as LegacyCertificationHarnessService

    assert LegacyCertificationHarnessService is CertificationHarnessService


def test_certification_harness_writes_artifacts_for_selected_matrix_cases(tmp_path: Path) -> None:
    report = CertificationHarnessService().run_mock_contract(
        artifact_dir=tmp_path,
        source="postgres",
        sink="mssql",
        strategy="snapshot_diff",
        row_count=100,
    )

    assert report.case_count == 1
    assert report.passed is True
    assert report.evidence_status == "UNVERIFIED"
    assert report.results[0].case_id == "postgres_to_mssql__snapshot_diff"
    assert (tmp_path / "postgres_to_mssql__snapshot_diff__behavior.json").exists()
    payload = json.loads((tmp_path / "certification_report.json").read_text())
    assert payload["passed"] is True
    assert payload["evidence_status"] == "UNVERIFIED"


def test_mock_certification_without_matching_cases_is_not_a_vacuous_success(tmp_path: Path) -> None:
    report = CertificationHarnessService().run_mock_contract(
        artifact_dir=tmp_path,
        source="unknown",
        sink="unknown",
        strategy="unknown",
    )

    assert report.case_count == 0
    assert report.passed is False
    assert report.evidence_status == "UNVERIFIED"


def test_shared_certification_reader_rejects_unverified_mock_success(tmp_path: Path) -> None:
    report = CertificationHarnessService().run_mock_contract(
        artifact_dir=tmp_path,
        source="postgres",
        sink="mssql",
        strategy="snapshot_diff",
    )

    item = CertificationArtifactReader().read(
        name="certification_report",
        path=tmp_path / "certification_report.json",
        required=True,
    )

    assert report.passed is True
    assert item is not None
    assert item.passed is False
    assert item.evidence_status == "UNVERIFIED"


def test_certification_automation_plan_lists_full_evidence_pipeline(tmp_path: Path) -> None:
    report = CertificationAutomationPlanService().build(
        output_dir=tmp_path / "automation",
        profile="mock_contract",
        row_count=10000,
    )

    assert report.passed is True
    assert report.profile == "mock_contract"
    assert report.row_count == 10000
    assert [step.name for step in report.steps] == [
        "source_sink_matrix",
        "matrix_report",
        "benchmark_baseline",
        "run_registry",
        "lineage_export",
        "evidence_bundle",
        "strategy_certification_bundle",
        "certification_suite",
        "artifact_index",
        "evidence_chain",
        "evidence_chain_verify",
    ]
    assert "certification_suite.json" in report.required_artifacts
    assert "evidence_chain_index.json" in report.required_artifacts
    assert (tmp_path / "automation" / "certification_automation_plan.json").exists()
    assert (tmp_path / "automation" / "certification_automation_plan.md").exists()


def test_certification_history_detects_regressions_and_fixed_cases(tmp_path: Path) -> None:
    previous = tmp_path / "previous.json"
    current = tmp_path / "current.json"
    previous.write_text(
        json.dumps(
            {
                "passed": False,
                "results": [
                    {"case_id": "postgres_to_mssql__snapshot_diff", "passed": True},
                    {"case_id": "mssql_to_clickhouse__incremental_merge", "passed": False},
                ],
            }
        ),
        encoding="utf-8",
    )
    current.write_text(
        json.dumps(
            {
                "passed": False,
                "results": [
                    {"case_id": "postgres_to_mssql__snapshot_diff", "passed": False},
                    {"case_id": "mssql_to_clickhouse__incremental_merge", "passed": True},
                    {"case_id": "postgres_to_clickhouse__replace", "passed": True},
                ],
            }
        ),
        encoding="utf-8",
    )

    report = CertificationHistoryService().record(
        history_dir=tmp_path / "history",
        release="v0.2.9",
        current_report_path=current,
        previous_report_path=previous,
    )

    assert report.status == "regression"
    assert report.badge == "regression"
    assert report.new_failures == ("postgres_to_mssql__snapshot_diff",)
    assert report.fixed_failures == ("mssql_to_clickhouse__incremental_merge",)
    assert (tmp_path / "history" / "v0.2.9__certification_history.json").exists()
    assert (tmp_path / "history" / "certification_history_index.json").exists()


def test_certification_history_marks_all_green_release_as_passing(tmp_path: Path) -> None:
    current = tmp_path / "current.json"
    current.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "PASS",
                "production_certification": "VERIFIED",
                "results": [
                    {"case_id": "postgres_to_mssql__snapshot_diff", "passed": True},
                    {"case_id": "mssql_to_clickhouse__incremental_merge", "passed": True},
                ],
            }
        ),
        encoding="utf-8",
    )

    report = CertificationHistoryService().record(
        history_dir=tmp_path / "history",
        release="v0.2.9",
        current_report_path=current,
    )

    assert report.passed is True
    assert report.status == "passing"
    assert report.badge == "passing"
    assert report.new_failures == ()


def test_certification_history_rejects_statusless_all_green_report(tmp_path: Path) -> None:
    current = tmp_path / "current.json"
    current.write_text(
        json.dumps(
            {
                "passed": True,
                "results": [{"case_id": "postgres_to_mssql__snapshot_diff", "passed": True}],
            }
        ),
        encoding="utf-8",
    )

    report = CertificationHistoryService().record(
        history_dir=tmp_path / "history",
        release="v0.2.9",
        current_report_path=current,
    )

    assert report.passed is False
    assert report.status != "passing"


def test_connector_badges_generate_connector_and_strategy_matrix(tmp_path: Path) -> None:
    history_index = tmp_path / "certification_history_index.json"
    history_index.write_text(
        json.dumps(
            {
                "releases": [
                    {
                        "release": "v0.2.9",
                        "status": "regression",
                        "badge": "regression",
                        "new_failures": ["postgres_to_mssql__snapshot_diff"],
                        "fixed_failures": ["mssql_to_clickhouse__incremental_merge"],
                        "unchanged_failures": [],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    report = ConnectorBadgeService().generate(
        output_dir=tmp_path / "badges",
        history_index_path=history_index,
    )

    entries = {entry.connector: entry for entry in report.entries}
    assert report.passed is False
    assert report.release == "v0.2.9"
    assert entries["postgres"].badge == "regression"
    assert entries["mssql"].badge == "regression"
    assert entries["clickhouse"].badge == "beta"
    assert report.strategy_rows[0].status == "new_failure"
    assert report.strategy_rows[0].source == "postgres"
    assert report.strategy_rows[0].sink == "mssql"
    assert report.strategy_rows[0].strategy == "snapshot_diff"
    assert (tmp_path / "badges" / "connector_badges.json").exists()
    assert (tmp_path / "badges" / "connector_badges.md").exists()


def test_release_gate_passes_when_all_artifacts_are_green(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    policy_path = tmp_path / "policy.json"
    slo_path = tmp_path / "slo.json"
    evidence_path.write_text(json.dumps({"passed": True}), encoding="utf-8")
    policy_path.write_text(json.dumps({"passed": True, "violations": []}), encoding="utf-8")
    slo_path.write_text(json.dumps({"passed": True, "results": []}), encoding="utf-8")

    report = ReleaseGateService().evaluate(
        artifact_dir=tmp_path / "release_gate",
        release="v0.2.9",
        artifacts={"evidence": evidence_path, "policy": policy_path, "slo": slo_path},
    )

    assert report.passed is True
    assert report.blockers == ()
    assert {item.name for item in report.items} == {"evidence", "policy", "slo"}
    assert all(len(item.sha256) == 64 for item in report.items)
    assert (tmp_path / "release_gate" / "release_gate.json").exists()
    assert (tmp_path / "release_gate" / "release_gate.md").exists()


def test_release_gate_fails_regression_and_missing_artifacts(tmp_path: Path) -> None:
    badges_path = tmp_path / "connector_badges.json"
    badges_path.write_text(
        json.dumps({"passed": False, "strategy_rows": [{"status": "new_failure"}]}), encoding="utf-8"
    )

    report = ReleaseGateService().evaluate(
        artifact_dir=tmp_path / "release_gate",
        release="v0.2.9",
        artifacts={"connector_badges": badges_path, "security": tmp_path / "missing.json"},
    )

    assert report.passed is False
    assert report.blockers == ("connector_badges", "security")
    assert "Fix release blockers before publishing" in report.to_markdown()


def test_release_gate_rejects_unverified_certification_under_generic_name(tmp_path: Path) -> None:
    certification = tmp_path / "mock-certification.json"
    certification.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "UNVERIFIED",
                "profile": "mock_contract",
            }
        ),
        encoding="utf-8",
    )

    report = ReleaseGateService().evaluate(
        artifact_dir=tmp_path / "release_gate",
        release="v0.73.17",
        artifacts={"evidence": certification},
    )

    assert report.passed is False
    assert report.blockers == ("evidence",)


def test_artifact_index_scans_roots_and_extracts_metadata(tmp_path: Path) -> None:
    root = tmp_path / "test_artifacts"
    release_dir = root / "ops" / "release"
    release_dir.mkdir(parents=True)
    (release_dir / "release_gate.json").write_text(
        json.dumps({"release": "v0.2.9", "passed": True, "blockers": []}),
        encoding="utf-8",
    )
    (release_dir / "ops_incident_pack.md").write_text("# incident\n", encoding="utf-8")

    report = ArtifactIndexService().build(
        output_dir=tmp_path / "index",
        roots=[root],
        release="v0.2.9",
    )

    items = {item.name: item for item in report.items}
    assert report.total_artifacts == 2
    assert report.total_bytes > 0
    assert items["release_gate.json"].artifact_type == "release_gate"
    assert items["release_gate.json"].release == "v0.2.9"
    assert items["release_gate.json"].passed is True
    assert len(items["release_gate.json"].sha256) == 64
    assert (tmp_path / "index" / "artifact_index.json").exists()
    assert (tmp_path / "index" / "artifact_index.md").exists()


def test_evidence_chain_appends_and_verifies_linked_entries(tmp_path: Path) -> None:
    index_1 = tmp_path / "artifact_index_1.json"
    index_2 = tmp_path / "artifact_index_2.json"
    index_1.write_text(json.dumps({"release": "v0.2.8", "passed": True}), encoding="utf-8")
    index_2.write_text(json.dumps({"release": "v0.2.9", "passed": True}), encoding="utf-8")

    service = EvidenceChainService()
    first = service.append(chain_dir=tmp_path / "chain", release="v0.2.8", artifact_index_path=index_1)
    second = service.append(
        chain_dir=tmp_path / "chain",
        release="v0.2.9",
        artifact_index_path=index_2,
        previous_entry_path=first.entry_path,
    )
    verification = service.verify(chain_dir=tmp_path / "chain")

    assert first.previous_chain_hash is None
    assert second.previous_chain_hash == first.chain_hash
    assert len(second.artifact_index_hash) == 64
    assert len(second.chain_hash) == 64
    assert verification.verified is True
    assert verification.broken_entries == ()
    assert (tmp_path / "chain" / "evidence_chain_index.json").exists()


def test_evidence_chain_detects_tampered_links(tmp_path: Path) -> None:
    index_1 = tmp_path / "artifact_index_1.json"
    index_2 = tmp_path / "artifact_index_2.json"
    index_1.write_text(json.dumps({"release": "v0.2.8", "passed": True}), encoding="utf-8")
    index_2.write_text(json.dumps({"release": "v0.2.9", "passed": True}), encoding="utf-8")

    service = EvidenceChainService()
    first = service.append(chain_dir=tmp_path / "chain", release="v0.2.8", artifact_index_path=index_1)
    second = service.append(
        chain_dir=tmp_path / "chain",
        release="v0.2.9",
        artifact_index_path=index_2,
        previous_entry_path=first.entry_path,
    )
    payload = json.loads(Path(second.entry_path).read_text(encoding="utf-8"))
    payload["previous_chain_hash"] = "0" * 64
    Path(second.entry_path).write_text(json.dumps(payload), encoding="utf-8")

    verification = service.verify(chain_dir=tmp_path / "chain")

    assert verification.verified is False
    assert verification.broken_entries == ("v0.2.9",)


def test_evidence_chain_verify_blocks_empty_chain(tmp_path: Path) -> None:
    verification = EvidenceChainService().verify(chain_dir=tmp_path / "empty-chain")

    assert verification.verified is False
    assert verification.entry_count == 0
    assert verification.broken_entries == ("chain.empty",)


def test_evidence_chain_rejects_missing_artifact_index(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text('{"passed": true}\n', encoding="utf-8")
    index = _write_bound_artifact_index(
        tmp_path / "artifact_index.json",
        release="v0.2.9",
        artifacts=(artifact,),
    )
    service = EvidenceChainService()
    service.append(
        chain_dir=tmp_path / "chain",
        release="v0.2.9",
        artifact_index_path=index,
    )
    index.unlink()

    verification = service.verify(
        chain_dir=tmp_path / "chain",
        expected_release="v0.2.9",
    )

    assert verification.verified is False
    assert verification.broken_entries == ("v0.2.9", "chain.artifact_index_unbound")


@pytest.mark.parametrize("mutation", ["delete", "tamper"])
def test_evidence_chain_revalidates_every_indexed_artifact(
    tmp_path: Path,
    mutation: str,
) -> None:
    replay = tmp_path / "replay.json"
    replay.write_text('{"passed": true}\n', encoding="utf-8")
    index = _write_bound_artifact_index(
        tmp_path / "artifact_index.json",
        release="v0.2.9",
        artifacts=(replay,),
    )
    service = EvidenceChainService()
    service.append(
        chain_dir=tmp_path / "chain",
        release="v0.2.9",
        artifact_index_path=index,
    )
    if mutation == "delete":
        replay.unlink()
    else:
        replay.write_text('{"passed": false}\n', encoding="utf-8")

    verification = service.verify(
        chain_dir=tmp_path / "chain",
        expected_release="v0.2.9",
    )

    assert verification.verified is False
    assert verification.broken_entries == ("chain.artifact_index_unbound",)


def test_evidence_chain_rejects_duplicate_indexed_artifact_identity(tmp_path: Path) -> None:
    replay = tmp_path / "replay.json"
    replay.write_text('{"passed": true}\n', encoding="utf-8")
    index = _write_bound_artifact_index(
        tmp_path / "artifact_index.json",
        release="v0.2.9",
        artifacts=(replay, replay),
    )
    service = EvidenceChainService()
    service.append(
        chain_dir=tmp_path / "chain",
        release="v0.2.9",
        artifact_index_path=index,
    )

    verification = service.verify(
        chain_dir=tmp_path / "chain",
        expected_release="v0.2.9",
    )

    assert verification.verified is False
    assert verification.broken_entries == ("chain.artifact_index_unbound",)


def test_release_orchestrator_builds_end_to_end_release_pack(tmp_path: Path) -> None:
    root = tmp_path / "ops-artifacts"
    root.mkdir()
    (root / "security_audit.json").write_text(json.dumps({"passed": True, "findings": []}), encoding="utf-8")
    (root / "slo_report.json").write_text(json.dumps({"passed": True, "results": []}), encoding="utf-8")

    report = ReleaseOrchestratorService().run(
        output_dir=tmp_path / "release",
        release="v0.2.9",
        roots=[root],
    )

    steps = {step.name: step for step in report.steps}
    assert report.passed is True
    assert report.blockers == ()
    assert set(steps) == {
        "artifact_index",
        "evidence_chain",
        "release_gate",
        "docs_publish_pack",
        "runbook_pack",
    }
    assert all(step.passed for step in steps.values())
    assert all(len(step.sha256) == 64 for step in steps.values())
    assert (tmp_path / "release" / "release_orchestration.json").exists()
    assert (tmp_path / "release" / "release_orchestration.md").exists()
    assert (tmp_path / "release" / "artifact-index" / "artifact_index.json").exists()
    assert (tmp_path / "release" / "evidence-chain" / "v0.2.9__evidence_chain.json").exists()
    assert (tmp_path / "release" / "runbook-pack" / "OPERATOR_RUNBOOK.md").exists()


def test_release_summary_combines_replay_matrix_and_connector_evidence(tmp_path: Path) -> None:
    release = "oss_rc_1"
    replay_artifact = tmp_path / "replay.json"
    replay_artifact.write_text(json.dumps({"passed": True}), encoding="utf-8")
    matrix_suite = tmp_path / "matrix_certification_suite.json"
    connector_suite = tmp_path / "connector_certification_suite.json"
    matrix_suite.write_text(
        json.dumps(
            {
                "schema_version": "dpone.certification_suite.v1",
                "release_id": release,
                "passed": True,
                "evidence_status": "PASS",
                "suite_id": "matrix-1",
            }
        ),
        encoding="utf-8",
    )
    connector_suite.write_text(
        json.dumps(
            {
                "schema_version": "dpone.certification_suite.v1",
                "release_id": release,
                "passed": True,
                "evidence_status": "PASS",
                "suite_id": "connectors-1",
            }
        ),
        encoding="utf-8",
    )
    replay_index = tmp_path / "replay_artifact_index.json"
    matrix_index = tmp_path / "matrix_artifact_index.json"
    connector_index = tmp_path / "connector_artifact_index.json"
    _write_bound_artifact_index(replay_index, release=release, artifacts=(replay_artifact,))
    _write_bound_artifact_index(matrix_index, release=release, artifacts=(matrix_suite,))
    _write_bound_artifact_index(connector_index, release=release, artifacts=(connector_suite,))
    EvidenceChainService().append(
        chain_dir=tmp_path / "replay-chain",
        release=release,
        artifact_index_path=replay_index,
    )
    EvidenceChainService().append(
        chain_dir=tmp_path / "matrix-chain",
        release=release,
        artifact_index_path=matrix_index,
    )
    EvidenceChainService().append(
        chain_dir=tmp_path / "connector-chain",
        release=release,
        artifact_index_path=connector_index,
    )

    report = ReleaseSummaryService().evaluate(
        output_dir=tmp_path / "summary",
        release_id=release,
        replay_chain_dir=tmp_path / "replay-chain",
        matrix_suite_path=matrix_suite,
        matrix_chain_dir=tmp_path / "matrix-chain",
        connector_suite_path=connector_suite,
        connector_chain_dir=tmp_path / "connector-chain",
    )

    assert report.passed is True
    assert report.release_id == "oss_rc_1"
    assert report.blockers == ()
    assert {item.name for item in report.items} == {
        "replay_evidence_chain",
        "matrix_certification_suite",
        "matrix_evidence_chain",
        "connector_certification_suite",
        "connector_evidence_chain",
    }
    assert (tmp_path / "summary" / "release_summary.json").exists()
    assert (tmp_path / "summary" / "release_summary.md").exists()


def test_release_summary_rejects_cross_release_replay(tmp_path: Path) -> None:
    current_release = "oss_rc_2"
    previous_release = "oss_rc_1"
    replay = tmp_path / "replay.json"
    replay.write_text('{"passed": true}\n', encoding="utf-8")
    replay_index = _write_bound_artifact_index(
        tmp_path / "replay_artifact_index.json",
        release=previous_release,
        artifacts=(replay,),
    )
    EvidenceChainService().append(
        chain_dir=tmp_path / "replay-chain",
        release=previous_release,
        artifact_index_path=replay_index,
    )
    suite_paths = []
    chain_paths = []
    for kind in ("matrix", "connector"):
        suite = tmp_path / f"{kind}_certification_suite.json"
        suite.write_text(
            json.dumps(
                {
                    "schema_version": "dpone.certification_suite.v1",
                    "release_id": current_release,
                    "suite_id": kind,
                    "passed": True,
                    "evidence_status": "PASS",
                    "blockers": [],
                }
            ),
            encoding="utf-8",
        )
        index = _write_bound_artifact_index(
            tmp_path / f"{kind}_artifact_index.json",
            release=current_release,
            artifacts=(suite,),
        )
        chain = tmp_path / f"{kind}-chain"
        EvidenceChainService().append(
            chain_dir=chain,
            release=current_release,
            artifact_index_path=index,
        )
        suite_paths.append(suite)
        chain_paths.append(chain)

    report = ReleaseSummaryService().evaluate(
        output_dir=tmp_path / "summary",
        release_id=current_release,
        replay_chain_dir=tmp_path / "replay-chain",
        matrix_suite_path=suite_paths[0],
        matrix_chain_dir=chain_paths[0],
        connector_suite_path=suite_paths[1],
        connector_chain_dir=chain_paths[1],
    )

    assert report.passed is False
    assert report.blockers == ("replay_evidence_chain.not_verified",)


def test_release_summary_blocks_red_suite_and_empty_chain(tmp_path: Path) -> None:
    release = "red_rc"
    matrix_suite = tmp_path / "matrix_certification_suite.json"
    connector_suite = tmp_path / "connector_certification_suite.json"
    matrix_suite.write_text(
        json.dumps(
            {
                "schema_version": "dpone.certification_suite.v1",
                "release_id": release,
                "suite_id": "matrix",
                "passed": False,
                "evidence_status": "FAIL",
                "blockers": ["case.red"],
            }
        ),
        encoding="utf-8",
    )
    connector_suite.write_text(
        json.dumps(
            {
                "schema_version": "dpone.certification_suite.v1",
                "release_id": release,
                "suite_id": "connectors",
                "passed": True,
                "evidence_status": "PASS",
            }
        ),
        encoding="utf-8",
    )
    matrix_index = _write_bound_artifact_index(
        tmp_path / "matrix_artifact_index.json",
        release=release,
        artifacts=(matrix_suite,),
    )
    connector_index = _write_bound_artifact_index(
        tmp_path / "connector_artifact_index.json",
        release=release,
        artifacts=(connector_suite,),
    )
    EvidenceChainService().append(
        chain_dir=tmp_path / "matrix-chain",
        release=release,
        artifact_index_path=matrix_index,
    )
    EvidenceChainService().append(
        chain_dir=tmp_path / "connector-chain",
        release=release,
        artifact_index_path=connector_index,
    )

    report = ReleaseSummaryService().evaluate(
        output_dir=tmp_path / "summary",
        release_id=release,
        replay_chain_dir=tmp_path / "empty-replay-chain",
        matrix_suite_path=matrix_suite,
        matrix_chain_dir=tmp_path / "matrix-chain",
        connector_suite_path=connector_suite,
        connector_chain_dir=tmp_path / "connector-chain",
    )

    assert report.passed is False
    assert report.blockers == (
        "replay_evidence_chain.not_verified",
        "matrix_certification_suite.not_passed",
    )


def test_release_promotion_records_green_environment_promotion(tmp_path: Path) -> None:
    release_orchestration = tmp_path / "release_orchestration.json"
    runbook = tmp_path / "runbook_pack.json"
    release_orchestration.write_text(json.dumps({"release": "v0.2.9", "passed": True}), encoding="utf-8")
    runbook.write_text(json.dumps({"passed": True, "blockers": []}), encoding="utf-8")

    report = ReleasePromotionService().promote(
        output_dir=tmp_path / "promotion",
        release="v0.2.9",
        from_environment="staging",
        to_environment="production",
        artifacts={"release_orchestration": release_orchestration, "runbook": runbook},
    )

    items = {item.name: item for item in report.items}
    assert report.passed is True
    assert report.blockers == ()
    assert report.from_environment == "staging"
    assert report.to_environment == "production"
    assert items["release_orchestration"].passed is True
    assert all(len(item.sha256) == 64 for item in report.items)
    assert (tmp_path / "promotion" / "promotion_manifest.json").exists()
    assert (tmp_path / "promotion" / "promotion_manifest.md").exists()


def test_release_promotion_blocks_missing_or_red_artifacts(tmp_path: Path) -> None:
    red = tmp_path / "release_orchestration.json"
    red.write_text(json.dumps({"release": "v0.2.9", "passed": False, "blockers": ["security"]}), encoding="utf-8")

    report = ReleasePromotionService().promote(
        output_dir=tmp_path / "promotion",
        release="v0.2.9",
        from_environment="staging",
        to_environment="production",
        artifacts={"release_orchestration": red, "rollback": tmp_path / "missing.json"},
    )

    assert report.passed is False
    assert report.blockers == ("release_orchestration", "rollback")
    assert "Release promotion blockers" in report.to_markdown()


@pytest.mark.parametrize(
    "payload",
    (
        {"passed": True},
        {"passed": True, "evidence_status": "UNVERIFIED"},
    ),
)
def test_release_consumers_reject_untrusted_certification(
    tmp_path: Path,
    payload: dict[str, object],
) -> None:
    certification = tmp_path / "certification.json"
    certification.write_text(json.dumps(payload), encoding="utf-8")
    deployment = tmp_path / "deployment_record.json"
    deployment.write_text(
        json.dumps(
            {
                "deployment_id": "dep-1",
                "release": "v1",
                "environment": "prod",
                "passed": True,
            }
        ),
        encoding="utf-8",
    )

    promotion = ReleasePromotionService().promote(
        output_dir=tmp_path / "promotion",
        release="v1",
        from_environment="stage",
        to_environment="prod",
        artifacts={"certification": certification},
    )
    change = ChangeRequestService().create(
        output_dir=tmp_path / "change",
        change_id="CR-1",
        release="v1",
        target_environment="prod",
        risk_level="medium",
        requested_by="release-manager",
        approvers=("platform-owner",),
        artifacts={"certification": certification},
    )
    post_deploy = PostDeployVerifyService().verify(
        output_dir=tmp_path / "post-deploy",
        deployment_record_path=deployment,
        artifacts={"certification": certification},
    )

    assert promotion.passed is False
    assert change.passed is False
    assert post_deploy.passed is False
    assert promotion.blockers == ("certification",)
    assert change.blockers == ("certification",)
    assert post_deploy.blockers == ("certification",)


def test_generic_release_boundaries_reject_stripped_certification_payload(tmp_path: Path) -> None:
    """A production decision cannot reinterpret stripped local evidence as legacy PASS."""

    certification = tmp_path / "route_live_certification.json"
    certification.write_text(
        json.dumps({"passed": True, "evidence_status": "PASS", "blockers": []}),
        encoding="utf-8",
    )

    gate = ReleaseGateService().evaluate(
        artifact_dir=tmp_path / "gate",
        release="v0.74.0",
        artifacts={"route_live_evidence_bundle": certification},
    )
    promotion = ReleasePromotionService().promote(
        output_dir=tmp_path / "promotion",
        release="v0.74.0",
        from_environment="local",
        to_environment="production",
        artifacts={"route_live_evidence_bundle": certification},
    )
    change = ChangeRequestService().create(
        output_dir=tmp_path / "change",
        change_id="CR-0740",
        release="v0.74.0",
        target_environment="production",
        risk_level="high",
        requested_by="release-manager",
        approvers=("platform-owner",),
        artifacts={"route_live_evidence_bundle": certification},
    )
    deployment = tmp_path / "deployment.json"
    deployment.write_text(
        json.dumps(
            {
                "deployment_id": "dep-0740",
                "release": "v0.74.0",
                "environment": "production",
                "passed": True,
            }
        ),
        encoding="utf-8",
    )
    post_deploy = PostDeployVerifyService().verify(
        output_dir=tmp_path / "post-deploy",
        deployment_record_path=deployment,
        artifacts={"route_live_evidence_bundle": certification},
    )
    industrial = IndustrialReadinessService().evaluate(
        output_dir=tmp_path / "industrial",
        release="v0.74.0",
        artifacts={"route_live_evidence_bundle": certification},
        required_domains=("route_live_evidence_bundle",),
    )

    assert gate.passed is False
    assert gate.blockers == ("route_live_evidence_bundle",)
    assert promotion.passed is False
    assert promotion.blockers == ("route_live_evidence_bundle",)
    assert change.passed is False
    assert post_deploy.passed is False
    assert industrial.passed is False

    certification.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "PASS",
                "production_certification": "VERIFIED",
                "blockers": [],
            }
        ),
        encoding="utf-8",
    )
    self_declared_gate = ReleaseGateService().evaluate(
        artifact_dir=tmp_path / "authorized-gate",
        release="v0.74.0",
        artifacts={"route_live_evidence_bundle": certification},
    )
    assert self_declared_gate.passed is False
    assert (
        production_artifact_payload_passed(
            {
                "local_behavior_passed": True,
                "local_evidence_status": "PASS",
                "environment_class": "LOCAL_DOCKER",
                "production_certification": "UNVERIFIED",
            },
            name="wide_release_campaign",
        )
        is False
    )
    for payload in (
        {
            "schema_version": "dpone.mssql-clickhouse-wide-release-authority.v1",
            "passed": True,
            "authority_sha256": "sha256:" + "b" * 64,
        },
        {
            "schema_version": "dpone.dbt_sqlserver.wide_materialization.v1",
            "passed": True,
            "evidence_sha256": "sha256:" + "c" * 64,
        },
        {"passed": True, "campaign_sha256": "sha256:" + "d" * 64},
        {"passed": True, "evidence_sha256": "sha256:" + "e" * 64},
    ):
        assert production_artifact_payload_passed(payload, name="supplemental_evidence") is False
    assert (
        production_artifact_payload_passed(
            {
                "schema_version": "dpone.mssql-clickhouse-wide-release-campaign.v1",
                "passed": True,
                "local_behavior_passed": True,
                "campaign_sha256": "sha256:" + "a" * 64,
            },
            name="supplemental_evidence",
        )
        is False
    )


@pytest.mark.parametrize(
    "payload",
    (
        {"passed": True},
        {"passed": True, "evidence_status": "UNVERIFIED"},
    ),
)
def test_release_summary_rejects_untrusted_certification_suite(
    tmp_path: Path,
    payload: dict[str, object],
) -> None:
    suite = tmp_path / "certification_suite.json"
    suite.write_text(json.dumps(payload), encoding="utf-8")

    item = ReleaseSummaryService()._suite_item("connector_certification_suite", suite)

    assert item.passed is False


def test_release_summary_fails_closed_for_malformed_certification_suite(tmp_path: Path) -> None:
    suite = tmp_path / "certification_suite.json"
    suite.write_text("{", encoding="utf-8")

    item = ReleaseSummaryService()._suite_item("connector_certification_suite", suite)

    assert item.passed is False


def test_environment_drift_allows_only_allowlisted_paths(tmp_path: Path) -> None:
    source = tmp_path / "staging.json"
    target = tmp_path / "production.json"
    source.write_text(
        json.dumps(
            {
                "release": "v0.2.9",
                "connection": {"host": "staging.db", "schema": "landing"},
                "strategy": {"mode": "incremental_merge"},
            }
        ),
        encoding="utf-8",
    )
    target.write_text(
        json.dumps(
            {
                "release": "v0.2.9",
                "connection": {"host": "prod.db", "schema": "landing"},
                "strategy": {"mode": "incremental_merge"},
            }
        ),
        encoding="utf-8",
    )

    report = EnvironmentDriftService().compare(
        output_dir=tmp_path / "drift",
        source_environment="staging",
        target_environment="production",
        source_path=source,
        target_path=target,
        allowlist_paths=("connection.host",),
    )

    assert report.passed is True
    assert report.blockers == ()
    assert report.allowed_drift_count == 1
    assert report.blocking_drift_count == 0
    assert report.differences[0].path == "connection.host"
    assert report.differences[0].allowed is True
    assert (tmp_path / "drift" / "env_drift.json").exists()
    assert (tmp_path / "drift" / "env_drift.md").exists()


def test_environment_drift_blocks_unapproved_changes(tmp_path: Path) -> None:
    source = tmp_path / "staging.json"
    target = tmp_path / "production.json"
    source.write_text(json.dumps({"strategy": {"mode": "incremental_merge"}}), encoding="utf-8")
    target.write_text(json.dumps({"strategy": {"mode": "replace"}}), encoding="utf-8")

    report = EnvironmentDriftService().compare(
        output_dir=tmp_path / "drift",
        source_environment="staging",
        target_environment="production",
        source_path=source,
        target_path=target,
        allowlist_paths=(),
    )

    assert report.passed is False
    assert report.blockers == ("strategy.mode",)
    assert report.blocking_drift_count == 1
    assert "Environment drift blockers" in report.to_markdown()


def test_change_request_builds_approval_artifact_from_green_inputs(tmp_path: Path) -> None:
    orchestration = tmp_path / "release_orchestration.json"
    drift = tmp_path / "env_drift.json"
    promotion = tmp_path / "promotion_manifest.json"
    orchestration.write_text(json.dumps({"release": "v0.2.9", "passed": True}), encoding="utf-8")
    drift.write_text(json.dumps({"passed": True, "blocking_drift_count": 0}), encoding="utf-8")
    promotion.write_text(json.dumps({"passed": True, "blockers": []}), encoding="utf-8")

    report = ChangeRequestService().create(
        output_dir=tmp_path / "change-request",
        change_id="CR-2026-0001",
        release="v0.2.9",
        target_environment="production",
        risk_level="medium",
        requested_by="release-manager@example.com",
        approvers=("data-architect@example.com", "platform-owner@example.com"),
        artifacts={
            "release_orchestration": orchestration,
            "env_drift": drift,
            "promotion": promotion,
        },
    )

    assert report.passed is True
    assert report.blockers == ()
    assert report.change_id == "CR-2026-0001"
    assert report.risk_level == "medium"
    assert report.approvers == ("data-architect@example.com", "platform-owner@example.com")
    assert all(len(item.sha256) == 64 for item in report.items)
    assert (tmp_path / "change-request" / "change_request.json").exists()
    assert (tmp_path / "change-request" / "change_request.md").exists()


def test_change_request_blocks_missing_approvers_and_red_artifacts(tmp_path: Path) -> None:
    drift = tmp_path / "env_drift.json"
    drift.write_text(json.dumps({"passed": False, "blockers": ["strategy.mode"]}), encoding="utf-8")

    report = ChangeRequestService().create(
        output_dir=tmp_path / "change-request",
        change_id="CR-2026-0002",
        release="v0.2.9",
        target_environment="production",
        risk_level="high",
        requested_by="release-manager@example.com",
        approvers=(),
        artifacts={"env_drift": drift, "rollback": tmp_path / "missing.json"},
    )

    assert report.passed is False
    assert report.blockers == ("approval.approvers_missing", "env_drift", "rollback")
    assert "Change request blockers" in report.to_markdown()


def test_approval_record_approves_change_request_when_quorum_is_met(tmp_path: Path) -> None:
    change_request = tmp_path / "change_request.json"
    change_request.write_text(
        json.dumps(
            {
                "change_id": "CR-2026-0001",
                "release": "v0.2.9",
                "target_environment": "production",
                "passed": True,
                "approvers": ["data-architect@example.com", "platform-owner@example.com"],
                "expires_at": None,
            }
        ),
        encoding="utf-8",
    )

    report = ApprovalRecordService().record(
        output_dir=tmp_path / "approval",
        change_request_path=change_request,
        actor="data-architect@example.com",
        decision="approved",
        comment="Release evidence reviewed.",
        quorum_required=1,
    )

    assert report.passed is True
    assert report.status == "approved"
    assert report.blockers == ()
    assert report.approvals_count == 1
    assert report.rejections_count == 0
    assert report.change_id == "CR-2026-0001"
    assert len(report.change_request_sha256) == 64
    assert (tmp_path / "approval" / "approval_record.json").exists()
    assert (tmp_path / "approval" / "approval_record.md").exists()


def test_approval_record_blocks_rejection_unauthorized_actor_and_expired_request(tmp_path: Path) -> None:
    change_request = tmp_path / "change_request.json"
    change_request.write_text(
        json.dumps(
            {
                "change_id": "CR-2026-0002",
                "release": "v0.2.9",
                "target_environment": "production",
                "passed": True,
                "approvers": ["data-architect@example.com"],
                "expires_at": "2020-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    report = ApprovalRecordService().record(
        output_dir=tmp_path / "approval",
        change_request_path=change_request,
        actor="unexpected@example.com",
        decision="rejected",
        comment="Not approved.",
        quorum_required=2,
    )

    assert report.passed is False
    assert report.status == "blocked"
    assert report.blockers == (
        "approval.actor_not_allowed",
        "approval.change_request_expired",
        "approval.rejected",
        "approval.quorum_not_met",
    )
    assert "Approval record blockers" in report.to_markdown()


def test_deployment_record_marks_successful_deployment_with_green_post_checks(tmp_path: Path) -> None:
    approval_record = tmp_path / "approval_record.json"
    approval_record.write_text(
        json.dumps(
            {
                "change_id": "CR-2026-0001",
                "release": "v0.2.9",
                "target_environment": "production",
                "status": "approved",
                "passed": True,
            }
        ),
        encoding="utf-8",
    )

    report = DeploymentRecordService().record(
        output_dir=tmp_path / "deployment",
        deployment_id="DEP-2026-0001",
        environment="production",
        actor="release-manager@example.com",
        approval_record_path=approval_record,
        status="succeeded",
        post_checks={"smoke": True, "quality": True},
    )

    assert report.passed is True
    assert report.status == "succeeded"
    assert report.blockers == ()
    assert report.deployment_id == "DEP-2026-0001"
    assert report.change_id == "CR-2026-0001"
    assert report.post_checks_count == 2
    assert report.failed_post_checks == ()
    assert len(report.approval_record_sha256) == 64
    assert (tmp_path / "deployment" / "deployment_record.json").exists()
    assert (tmp_path / "deployment" / "deployment_record.md").exists()


def test_deployment_record_blocks_failed_deployment_red_approval_and_post_checks(tmp_path: Path) -> None:
    approval_record = tmp_path / "approval_record.json"
    approval_record.write_text(
        json.dumps(
            {
                "change_id": "CR-2026-0002",
                "release": "v0.2.9",
                "target_environment": "production",
                "status": "blocked",
                "passed": False,
            }
        ),
        encoding="utf-8",
    )

    report = DeploymentRecordService().record(
        output_dir=tmp_path / "deployment",
        deployment_id="DEP-2026-0002",
        environment="production",
        actor="release-manager@example.com",
        approval_record_path=approval_record,
        status="failed",
        post_checks={"smoke": True, "quality": False},
    )

    assert report.passed is False
    assert report.blockers == (
        "deployment.approval_not_passed",
        "deployment.status_failed",
        "post_check.quality",
    )
    assert report.failed_post_checks == ("quality",)
    assert "Deployment record blockers" in report.to_markdown()


def test_post_deploy_verify_closes_release_when_all_checks_are_green(tmp_path: Path) -> None:
    deployment_record = tmp_path / "deployment_record.json"
    slo = tmp_path / "slo_report.json"
    diff = tmp_path / "diff.json"
    deployment_record.write_text(
        json.dumps(
            {
                "deployment_id": "DEP-2026-0001",
                "release": "v0.2.9",
                "environment": "production",
                "passed": True,
            }
        ),
        encoding="utf-8",
    )
    slo.write_text(json.dumps({"passed": True, "results": []}), encoding="utf-8")
    diff.write_text(json.dumps({"passed": True, "samples": []}), encoding="utf-8")

    report = PostDeployVerifyService().verify(
        output_dir=tmp_path / "post-deploy",
        deployment_record_path=deployment_record,
        artifacts={"slo": slo, "diff": diff},
    )

    assert report.passed is True
    assert report.status == "release_closed"
    assert report.blockers == ()
    assert report.deployment_id == "DEP-2026-0001"
    assert report.release == "v0.2.9"
    assert report.environment == "production"
    assert report.checks_count == 3
    assert all(len(item.sha256) == 64 for item in report.items)
    assert (tmp_path / "post-deploy" / "post_deploy_verify.json").exists()
    assert (tmp_path / "post-deploy" / "post_deploy_verify.md").exists()


def test_post_deploy_verify_requires_rollback_for_red_or_missing_checks(tmp_path: Path) -> None:
    deployment_record = tmp_path / "deployment_record.json"
    security = tmp_path / "security_audit.json"
    deployment_record.write_text(
        json.dumps(
            {
                "deployment_id": "DEP-2026-0002",
                "release": "v0.2.9",
                "environment": "production",
                "passed": False,
            }
        ),
        encoding="utf-8",
    )
    security.write_text(json.dumps({"passed": False, "findings": [{"code": "secret"}]}), encoding="utf-8")

    report = PostDeployVerifyService().verify(
        output_dir=tmp_path / "post-deploy",
        deployment_record_path=deployment_record,
        artifacts={"security": security, "contract": tmp_path / "missing.json"},
    )

    assert report.passed is False
    assert report.status == "rollback_required"
    assert report.blockers == ("deployment_record", "contract", "security")
    assert "Post-deploy verification blockers" in report.to_markdown()


def test_release_close_closes_green_post_deploy_gate(tmp_path: Path) -> None:
    post_deploy = tmp_path / "post_deploy_verify.json"
    post_deploy.write_text(
        json.dumps(
            {
                "deployment_id": "DEP-2026-0001",
                "release": "v0.2.9",
                "environment": "production",
                "status": "release_closed",
                "passed": True,
            }
        ),
        encoding="utf-8",
    )

    report = ReleaseCloseService().close(
        output_dir=tmp_path / "release-close",
        release="v0.2.9",
        closed_by="release-manager@example.com",
        post_deploy_verify_path=post_deploy,
        notes="Release closed after green post-deploy checks.",
    )

    assert report.passed is True
    assert report.status == "closed"
    assert report.blockers == ()
    assert report.release == "v0.2.9"
    assert report.deployment_id == "DEP-2026-0001"
    assert report.environment == "production"
    assert len(report.post_deploy_verify_sha256) == 64
    assert (tmp_path / "release-close" / "release_close.json").exists()
    assert (tmp_path / "release-close" / "release_close.md").exists()


def test_release_close_requires_rollback_for_red_or_missing_gate(tmp_path: Path) -> None:
    red_gate = tmp_path / "post_deploy_verify.json"
    red_gate.write_text(
        json.dumps(
            {
                "deployment_id": "DEP-2026-0002",
                "release": "v0.2.9",
                "environment": "production",
                "status": "rollback_required",
                "passed": False,
            }
        ),
        encoding="utf-8",
    )

    report = ReleaseCloseService().close(
        output_dir=tmp_path / "release-close",
        release="v0.2.9",
        closed_by="release-manager@example.com",
        post_deploy_verify_path=red_gate,
    )

    missing_report = ReleaseCloseService().close(
        output_dir=tmp_path / "missing-close",
        release="v0.2.9",
        closed_by="release-manager@example.com",
        post_deploy_verify_path=tmp_path / "missing.json",
    )

    assert report.passed is False
    assert report.status == "rollback_required"
    assert report.blockers == ("release_close.post_deploy_not_passed",)
    assert missing_report.blockers == ("release_close.post_deploy_missing",)
    assert "Release close blockers" in report.to_markdown()


def test_docs_publish_pack_generates_pages_and_readme_snippets(tmp_path: Path) -> None:
    artifact_index = tmp_path / "artifact_index.json"
    connector_badges = tmp_path / "connector_badges.json"
    release_gate = tmp_path / "release_gate.json"
    artifact_index.write_text(
        json.dumps({"passed": True, "total_artifacts": 3, "items": []}),
        encoding="utf-8",
    )
    connector_badges.write_text(
        json.dumps(
            {
                "passed": True,
                "entries": [
                    {"connector": "postgres", "badge": "certified", "docs": "[PostgreSQL](postgres.md)"},
                    {"connector": "mssql", "badge": "beta", "docs": "[MSSQL](mssql.md)"},
                ],
            }
        ),
        encoding="utf-8",
    )
    release_gate.write_text(json.dumps({"passed": True, "blockers": []}), encoding="utf-8")

    report = DocsPublishPackService().build(
        output_dir=tmp_path / "publish",
        release="v0.2.9",
        artifacts={
            "artifact_index": artifact_index,
            "connector_badges": connector_badges,
            "release_gate": release_gate,
        },
    )

    assert report.passed is True
    assert report.blockers == ()
    assert {section.name for section in report.sections} == {"artifact_index", "connector_badges", "release_gate"}
    assert (tmp_path / "publish" / "docs_publish_pack.json").exists()
    assert (tmp_path / "publish" / "docs_publish_pack.md").exists()
    assert (tmp_path / "publish" / "README_SNIPPET.md").exists()
    assert (tmp_path / "publish" / "PAGES_INDEX.md").exists()
    assert "postgres" in (tmp_path / "publish" / "README_SNIPPET.md").read_text(encoding="utf-8")


def test_docs_publish_pack_blocks_red_release_gate(tmp_path: Path) -> None:
    release_gate = tmp_path / "release_gate.json"
    release_gate.write_text(json.dumps({"passed": False, "blockers": ["security"]}), encoding="utf-8")

    report = DocsPublishPackService().build(
        output_dir=tmp_path / "publish",
        release="v0.2.9",
        artifacts={"release_gate": release_gate},
    )

    assert report.passed is False
    assert report.blockers == ("release_gate",)


def test_manifest_bundle_redacts_copies_and_checksums_files(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yml"
    release_gate = tmp_path / "release_gate.json"
    manifest.write_text(
        "source:\n  credentials:\n    password: plain-text-password\n",
        encoding="utf-8",
    )
    release_gate.write_text(json.dumps({"passed": True}), encoding="utf-8")

    report = ManifestBundleService().build(
        output_dir=tmp_path / "bundle",
        bundle_id="bundle_01",
        files={"manifest": manifest, "release_gate": release_gate},
        redact=True,
    )

    copied = tmp_path / "bundle" / "files" / "manifest.yml"
    assert report.passed is True
    assert {item.name for item in report.items} == {"manifest", "release_gate"}
    assert copied.exists()
    assert "plain-text-password" not in copied.read_text(encoding="utf-8")
    assert "[REDACTED]" in copied.read_text(encoding="utf-8")
    assert all(len(item.sha256) == 64 for item in report.items)
    assert (tmp_path / "bundle" / "manifest_bundle.json").exists()
    assert (tmp_path / "bundle" / "manifest_bundle.md").exists()


def test_manifest_bundle_blocks_missing_files(tmp_path: Path) -> None:
    report = ManifestBundleService().build(
        output_dir=tmp_path / "bundle",
        bundle_id="bundle_02",
        files={"missing": tmp_path / "missing.yml"},
    )

    assert report.passed is False
    assert report.blockers == ("missing",)


def test_runbook_pack_generates_operator_steps_and_blockers(tmp_path: Path) -> None:
    release_gate = tmp_path / "release_gate.json"
    security = tmp_path / "security.json"
    rollback = tmp_path / "rollback_execute.json"
    release_gate.write_text(json.dumps({"passed": False, "blockers": ["security"]}), encoding="utf-8")
    security.write_text(json.dumps({"passed": False, "findings": [{"code": "secret"}]}), encoding="utf-8")
    rollback.write_text(
        json.dumps({"passed": True, "dry_run": True, "post_actions": ["Run `dpone ops diff`"]}),
        encoding="utf-8",
    )

    report = RunbookPackService().build(
        output_dir=tmp_path / "runbook",
        runbook_id="runbook_01",
        title="Release go-live",
        artifacts={"release_gate": release_gate, "security": security, "rollback": rollback},
    )

    assert report.passed is False
    assert report.blockers == ("release_gate", "security")
    assert {section.name for section in report.sections} == {"release_gate", "security", "rollback"}
    operator_runbook = tmp_path / "runbook" / "OPERATOR_RUNBOOK.md"
    assert operator_runbook.exists()
    text = operator_runbook.read_text(encoding="utf-8")
    assert "Go-live checklist" in text
    assert "Rollback checklist" in text
    assert "Run `dpone ops diff`" in text
    assert (tmp_path / "runbook" / "runbook_pack.json").exists()
    assert (tmp_path / "runbook" / "runbook_pack.md").exists()


def test_runbook_pack_passes_green_artifacts(tmp_path: Path) -> None:
    release_gate = tmp_path / "release_gate.json"
    slo = tmp_path / "slo.json"
    release_gate.write_text(json.dumps({"passed": True, "blockers": []}), encoding="utf-8")
    slo.write_text(json.dumps({"passed": True, "results": []}), encoding="utf-8")

    report = RunbookPackService().build(
        output_dir=tmp_path / "runbook",
        runbook_id="runbook_02",
        title="Release go-live",
        artifacts={"release_gate": release_gate, "slo": slo},
    )

    assert report.passed is True
    assert report.blockers == ()


def test_data_contract_service_fails_or_warns_with_actionable_results() -> None:
    contract = {
        "required_columns": ["id", "email"],
        "not_null": ["id", "email"],
        "unique": ["id"],
        "min_rows": 3,
        "max_null_ratio": {"email": 0.10},
    }
    rows = [{"id": 1, "email": "a@example.com"}, {"id": 1, "email": None}]

    report = DataContractService().evaluate(rows, contract, mode="fail")

    assert report.passed is False
    assert {item.code for item in report.results} >= {
        "min_rows",
        "not_null.email",
        "unique.id",
        "max_null_ratio.email",
    }
    assert all(item.severity in {"fail", "pass"} for item in report.results)
    assert "Fix duplicate values" in report.to_markdown()


def test_legacy_quarantine_service_exports_but_requires_real_replay_executor(tmp_path: Path) -> None:
    service = QuarantineService(tmp_path)
    entry = service.put(
        run_id="run_01",
        load_id="load_01",
        row={"id": 1, "amount": "bad"},
        reason="type_error",
        diagnostics={"column": "amount", "expected": "decimal"},
    )

    assert len(entry.quarantine_id) == 26
    exported = service.export(run_id="run_01")
    assert exported.total_rows == 1
    assert exported.entries[0].reason == "type_error"

    replay = service.replay(run_id="run_01", yes=True)
    assert replay.replayed_rows == 0
    assert replay.applied is False
    assert replay.error_code == "DPONE_DLQ_REPLAY_EXECUTOR_REQUIRED"


def test_load_package_service_enforces_commit_before_state_advance(tmp_path: Path) -> None:
    service = LoadPackageService(tmp_path)
    package = service.start(run_id="run_01", target="landing.orders", chunk_id="2026-06-01")
    service.mark_staged(package.load_id, rows_staged=10, artifact_uri="file://stage")

    with pytest.raises(ValueError, match="committed"):
        service.state_commit_payload(package.load_id)

    committed = service.mark_committed(package.load_id, rows_loaded=10, state_after={"xmin": "42"})

    assert committed.status is LoadPackageStatus.COMMITTED
    assert service.state_commit_payload(package.load_id) == {"xmin": "42"}


def test_rollback_plan_requires_yes_for_apply_and_renders_target_native_steps() -> None:
    service = RollbackPlanService()
    plan = service.plan(
        sink="mssql",
        target="landing.orders",
        load_id="01HV0000000000000000000000",
        strategy="shadow_swap",
    )

    assert plan.actions[0].kind == "validate_backup"
    assert any("sp_rename" in action.sql for action in plan.actions)
    assert service.apply(plan, yes=False).applied is False
    assert service.apply(plan, yes=True).applied is True


def test_rollback_execution_dry_run_builds_lock_and_post_actions() -> None:
    report = RollbackExecutionService().execute(
        sink="mssql",
        target="landing.orders",
        load_id="01JLOAD0000000000000000000",
        strategy="shadow_swap",
        yes=False,
        require_backup=True,
    )

    assert report.dry_run is True
    assert report.applied is False
    assert report.backup_validated is True
    assert report.lock_id.startswith("rollback:mssql:landing.orders:")
    assert "Run `dpone ops diff`" in report.post_actions
    assert "dry-run" in report.to_markdown()


def test_rollback_execution_apply_requires_explicit_yes() -> None:
    report = RollbackExecutionService().execute(
        sink="mssql",
        target="landing.orders",
        load_id="01JLOAD0000000000000000000",
        strategy="shadow_swap",
        yes=True,
    )

    assert report.dry_run is False
    assert report.applied is True
    assert report.planned_actions > 0


def test_connector_marketplace_delegates_to_canonical_capability_snapshot(
    tmp_path: Path,
) -> None:
    snapshot = build_capability_discovery_service(root=tmp_path).snapshot()
    marketplace = ConnectorMarketplaceService.default(snapshot)
    catalog = marketplace.catalog()
    payload = catalog.to_dict()

    assert {"postgres", "mssql", "clickhouse", "kafka"} <= set(catalog.connectors)
    assert payload["schema"] == "dpone.connector-marketplace.v2"
    assert catalog.connectors["postgres"].status == "beta"
    connectors = {item.id: item for item in snapshot.connectors}
    assert set(catalog.connectors) == set(connectors)
    assert all(
        catalog.connectors[connector_id].status == connector.release_phase
        and catalog.connectors[connector_id].release_phase == connector.release_phase
        and catalog.connectors[connector_id].maturity == connector.maturity
        and catalog.connectors[connector_id].capabilities == connector.capability_ids
        for connector_id, connector in connectors.items()
    )
    markdown = catalog.to_markdown()
    assert "| connector | status | capabilities | docs |" in markdown
    assert "[PostgreSQL](source-sink/postgres-to-clickhouse.md)" in markdown


def test_marketplace_and_evidence_bundle_propagate_capability_issues(
    tmp_path: Path,
) -> None:
    (tmp_path / "dpone.yaml").write_text(
        """
schema: dpone.project.v1
capability_discovery:
  certification_evidence:
    matrix_path: ../outside.json
    expected_commit: not-a-commit
    evidence_dirs: []
    max_age_hours: 0
""".lstrip(),
        encoding="utf-8",
    )
    snapshot = build_capability_discovery_service(root=tmp_path).snapshot()
    marketplace = ConnectorMarketplaceService.default(snapshot)

    catalog = marketplace.catalog()
    bundle = EvidenceBundleService(marketplace=marketplace).build(
        artifact_dir=tmp_path / "evidence",
        run_id="run_capability_issue",
        source="postgres",
        sink="mssql",
        strategy="snapshot_diff",
        rows=[{"id": 1}],
        contract={"required_columns": ["id"], "not_null": ["id"]},
        row_count=100,
    )

    assert catalog.passed is False
    assert catalog.to_dict()["issues"][0]["code"] == ("DPONE_CAPABILITY_EVIDENCE_CONFIG_INVALID")
    marketplace_item = next(item for item in bundle.items if item.name == "marketplace")
    assert marketplace_item.passed is False
    assert bundle.passed is False


def test_marketplace_entry_preserves_legacy_constructor_defaults() -> None:
    entry = ConnectorCatalogEntry(
        "legacy",
        "beta",
        ("full_refresh",),
        "legacy.md",
        "beta",
    )

    assert entry.maturity == "experimental"
    assert entry.release_phase == "beta"


def test_evidence_bundle_collects_certification_contract_and_marketplace_artifacts(tmp_path: Path) -> None:
    bundle = EvidenceBundleService().build(
        artifact_dir=tmp_path,
        run_id="run_01",
        source="postgres",
        sink="mssql",
        strategy="snapshot_diff",
        rows=[{"id": 1, "email": "a@example.com"}],
        contract={"required_columns": ["id", "email"], "not_null": ["id"]},
        row_count=100,
    )

    assert bundle.passed is False
    assert {item.name for item in bundle.items} == {"certification", "data_contract", "marketplace"}
    assert next(item for item in bundle.items if item.name == "certification").passed is False
    assert all(len(item.sha256) == 64 for item in bundle.items)
    assert (tmp_path / "ops_evidence_bundle.json").exists()
    assert (tmp_path / "ops_evidence_bundle.md").exists()


def test_go_live_gate_fails_when_required_evidence_fails(tmp_path: Path) -> None:
    bundle = EvidenceBundleService().build(
        artifact_dir=tmp_path,
        run_id="run_02",
        source="postgres",
        sink="mssql",
        strategy="snapshot_diff",
        rows=[{"id": 1, "email": None}],
        contract={"required_columns": ["id", "email"], "not_null": ["email"]},
        row_count=100,
    )

    decision = GoLiveGateService().evaluate(bundle)

    assert decision.passed is False
    assert "certification" in decision.blockers
    assert "data_contract" in decision.blockers
    assert "Fix failed evidence before go-live" in decision.to_markdown()


def test_deserialized_statusless_certification_cannot_pass_go_live_or_policy(tmp_path: Path) -> None:
    certification = tmp_path / "certification.json"
    certification.write_text('{"passed": true}\n', encoding="utf-8")
    payload = {
        "run_id": "untrusted",
        "passed": True,
        "items": [
            {
                "name": "certification",
                "path": str(certification),
                "sha256": "0" * 64,
                "required": True,
                "passed": True,
            }
        ],
    }

    bundle = OpsEvidenceBundle.from_dict(payload)
    gate = GoLiveGateService().evaluate(bundle)
    policy = OpsPolicyService().evaluate(bundle, {"required_evidence": ["certification"]})

    assert bundle.passed is False
    assert gate.passed is False
    assert gate.blockers == ("certification",)
    assert policy.passed is False
    assert "bundle.failed" in policy.violations
    assert "required_evidence_failed.certification" in policy.violations


def test_statusless_certification_is_blocked_by_operational_projection_surfaces(tmp_path: Path) -> None:
    certification = tmp_path / "source" / "strategy_certification_bundle.json"
    certification.parent.mkdir()
    certification.write_text(
        json.dumps(
            {
                "schema_version": "dpone.strategy.certification_bundle.v1",
                "passed": True,
                "blockers": [],
            }
        ),
        encoding="utf-8",
    )

    artifact_index = ArtifactIndexService().build(
        output_dir=tmp_path / "index",
        roots=(certification.parent,),
    )
    docs = DocsPublishPackService().build(
        output_dir=tmp_path / "docs",
        release="v0.73.18",
        artifacts={"certification": certification},
    )
    runbook = RunbookPackService().build(
        output_dir=tmp_path / "runbook",
        runbook_id="certification-review",
        title="Certification review",
        artifacts={"certification": certification},
    )
    incident = OpsIncidentPackService().build(
        artifact_dir=tmp_path / "incident",
        incident_id="certification-review",
        title="Certification review",
        artifacts={"certification": certification},
    )
    industrial = IndustrialReadinessService().evaluate(
        output_dir=tmp_path / "industrial",
        release="v0.73.18",
        artifacts={"certification": certification},
        required_domains=("certification",),
    )

    assert artifact_index.passed is False
    assert docs.passed is False
    assert runbook.passed is False
    assert incident.passed is False
    assert industrial.passed is False


def test_ops_policy_service_fails_missing_required_evidence(tmp_path: Path) -> None:
    bundle = EvidenceBundleService().build(
        artifact_dir=tmp_path,
        run_id="run_03",
        source="postgres",
        sink="mssql",
        strategy="snapshot_diff",
        rows=[{"id": 1, "email": "a@example.com"}],
        contract={"required_columns": ["id"]},
        row_count=100,
    )

    decision = OpsPolicyService().evaluate(bundle, {"required_evidence": ["certification", "lineage_audit"]})

    assert decision.passed is False
    assert "missing_required_evidence.lineage_audit" in decision.violations
    assert "Add `lineage_audit` to the evidence bundle" in decision.to_markdown()


def test_ops_policy_service_passes_required_evidence_and_artifact_checks(tmp_path: Path) -> None:
    certification = tmp_path / "certification.json"
    contract = tmp_path / "data_contract.json"
    certification.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "PASS",
                "production_certification": "VERIFIED",
                "blockers": [],
            }
        ),
        encoding="utf-8",
    )
    contract.write_text(json.dumps({"passed": True, "blockers": []}), encoding="utf-8")
    bundle = OpsEvidenceBundle(
        run_id="run_04",
        passed=True,
        items=(
            EvidenceItem(
                name="certification",
                path=str(certification),
                sha256=sha256_file(certification),
                required=True,
                passed=True,
                evidence_status="PASS",
            ),
            EvidenceItem(
                name="data_contract",
                path=str(contract),
                sha256=sha256_file(contract),
                required=True,
                passed=True,
            ),
        ),
    )

    decision = OpsPolicyService().evaluate(
        bundle,
        {"required_evidence": ["certification", "data_contract"], "require_artifacts_exist": True},
    )

    assert decision.passed is True
    assert decision.violations == ()


def test_ops_evidence_integrity_blocks_empty_missing_tampered_duplicate_and_contradictory_items(
    tmp_path: Path,
) -> None:
    gate = GoLiveGateService()
    policy = OpsPolicyService()
    empty = OpsEvidenceBundle(run_id="empty", passed=True, items=tuple())

    assert gate.evaluate(empty).passed is False
    assert "bundle.empty" in policy.evaluate(empty, {}).violations

    certification = tmp_path / "certification.json"
    certification.write_text(
        json.dumps({"passed": True, "evidence_status": "PASS", "blockers": []}),
        encoding="utf-8",
    )
    valid = EvidenceItem(
        name="certification",
        path=str(certification),
        sha256=sha256_file(certification),
        required=True,
        passed=True,
        evidence_status="PASS",
    )
    duplicate = OpsEvidenceBundle(run_id="duplicate", passed=True, items=(valid, valid))
    duplicate_decision = policy.evaluate(duplicate, {"required_evidence": ["certification"]})
    assert duplicate_decision.passed is False
    assert "duplicate_required_evidence.certification" in duplicate_decision.violations

    certification.write_text(
        json.dumps({"passed": True, "evidence_status": "PASS", "blockers": ["forged"]}),
        encoding="utf-8",
    )
    tampered = OpsEvidenceBundle(run_id="tampered", passed=True, items=(valid,))
    tampered_decision = policy.evaluate(tampered, {"required_evidence": ["certification"]})
    assert gate.evaluate(tampered).passed is False
    assert tampered_decision.passed is False
    assert "required_evidence_failed.certification" in tampered_decision.violations
    assert "artifact_untrusted.certification" in tampered_decision.violations

    missing = OpsEvidenceBundle(
        run_id="missing",
        passed=True,
        items=(
            EvidenceItem(
                name="certification",
                path=str(tmp_path / "missing.json"),
                sha256="0" * 64,
                required=True,
                passed=True,
                evidence_status="PASS",
            ),
        ),
    )
    missing_decision = policy.evaluate(missing, {"required_evidence": ["certification"]})
    assert missing_decision.passed is False
    assert "artifact_missing.certification" in missing_decision.violations


def test_ops_diff_service_reports_missing_extra_and_mismatched_rows() -> None:
    report = OpsDiffService().compare(
        source_rows=[
            {"id": 1, "amount": 100, "status": "paid"},
            {"id": 2, "amount": 200, "status": "paid"},
            {"id": 3, "amount": 300, "status": "new"},
        ],
        target_rows=[
            {"id": 1, "amount": 100, "status": "paid"},
            {"id": 2, "amount": 250, "status": "paid"},
            {"id": 4, "amount": 400, "status": "extra"},
        ],
        key_columns=["id"],
        compare_columns=["amount", "status"],
        sample_limit=10,
    )

    assert report.passed is False
    assert report.missing_in_target_count == 1
    assert report.extra_in_target_count == 1
    assert report.mismatch_count == 1
    assert {sample.kind for sample in report.samples} == {"missing_in_target", "extra_in_target", "mismatch"}
    assert "Fix source-target drift before state commit" in report.to_markdown()


def test_ops_diff_service_passes_equal_rows_regardless_input_order() -> None:
    report = OpsDiffService().compare(
        source_rows=[{"id": 2, "amount": 200}, {"id": 1, "amount": 100}],
        target_rows=[{"id": 1, "amount": 100}, {"id": 2, "amount": 200}],
        key_columns=["id"],
    )

    assert report.passed is True
    assert report.source_count == 2
    assert report.target_count == 2
    assert report.samples == ()


def test_ops_security_audit_fails_inline_secrets_and_unredacted_logs() -> None:
    report = OpsSecurityAuditService().audit(
        manifest={
            "source": {
                "connection_type": "params",
                "credentials": {"password": "plain-text-password"},
            }
        },
        log_text="starting connector with token=ghp_example1234567890abcdef",
    )

    assert report.passed is False
    assert "manifest.inline_secret.source.credentials.password" in {finding.code for finding in report.findings}
    assert "log.unredacted_secret.github_token" in {finding.code for finding in report.findings}
    assert "Move secret to env, Vault, Airflow, or external credential provider" in report.to_markdown()


def test_ops_security_audit_passes_provider_references_and_redacted_logs() -> None:
    report = OpsSecurityAuditService().audit(
        manifest={
            "source": {
                "connection_type": "vault",
                "credentials": {
                    "password": "${DPONE_SOURCE_PASSWORD}",
                    "api_key": "vault:secret/data/dpone/api#key",
                },
            }
        },
        log_text="password=[REDACTED] token=[REDACTED]",
    )

    assert report.passed is True
    assert report.findings[0].code == "security.no_findings"


def test_ops_slo_service_fails_threshold_breaches_with_actions() -> None:
    report = OpsSloService().evaluate(
        metrics={
            "freshness_lag_seconds": 900,
            "throughput_rows_per_second": 250,
            "p95_latency_seconds": 45,
            "failure_rate": 0.02,
        },
        objectives={
            "freshness_lag_seconds": {"max": 300},
            "throughput_rows_per_second": {"min": 1000},
            "p95_latency_seconds": {"max": 60},
            "failure_rate": {"max": 0.01},
        },
    )

    assert report.passed is False
    assert {item.code for item in report.results if not item.passed} == {
        "slo.freshness_lag_seconds.max",
        "slo.throughput_rows_per_second.min",
        "slo.failure_rate.max",
    }
    assert "Tune partitioning, native bulk path, batch size, or retry policy" in report.to_markdown()


def test_ops_slo_service_passes_when_metrics_meet_objectives() -> None:
    report = OpsSloService().evaluate(
        metrics={
            "freshness_lag_seconds": 120,
            "throughput_rows_per_second": 2500,
            "retry_budget_used_ratio": 0.10,
        },
        objectives={
            "freshness_lag_seconds": {"max": 300},
            "throughput_rows_per_second": {"min": 1000},
            "retry_budget_used_ratio": {"max": 0.20},
        },
    )

    assert report.passed is True
    assert all(item.passed for item in report.results)


def test_ops_incident_pack_collects_artifacts_and_surfaces_blockers(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    policy_path = tmp_path / "policy.json"
    evidence_path.write_text(json.dumps({"passed": True}), encoding="utf-8")
    policy_path.write_text(
        json.dumps({"passed": False, "violations": ["missing_required_evidence.slo"]}),
        encoding="utf-8",
    )

    report = OpsIncidentPackService().build(
        artifact_dir=tmp_path / "pack",
        incident_id="inc_01",
        title="Release gate review",
        severity="release",
        artifacts={"evidence": evidence_path, "policy": policy_path},
    )

    assert report.passed is False
    assert {item.name for item in report.items} == {"evidence", "policy"}
    assert all(len(item.sha256) == 64 for item in report.items)
    assert (tmp_path / "pack" / "ops_incident_pack.json").exists()
    assert (tmp_path / "pack" / "ops_incident_pack.md").exists()
    assert "Fix failed incident pack items before close/go-live" in report.to_markdown()


def test_run_registry_records_green_run_result_and_artifacts(tmp_path: Path) -> None:
    run_result = tmp_path / "run_result.json"
    quality = tmp_path / "quality.json"
    run_result.write_text(
        json.dumps(
            {
                "run_id": "run_01",
                "process": "orders",
                "manifest": "examples/postgres_to_mssql.yml",
                "passed": True,
                "result": {"status": "success"},
            }
        ),
        encoding="utf-8",
    )
    quality.write_text(json.dumps({"passed": True, "checks": []}), encoding="utf-8")

    report = RunRegistryService().record(
        output_dir=tmp_path / "registry",
        run_result_path=run_result,
        artifacts={"quality": quality},
    )

    assert report.passed is True
    assert report.run_id == "run_01"
    assert report.process == "orders"
    assert report.status == "success"
    assert report.artifact_count == 1
    assert len(report.run_result_sha256) == 64
    assert report.artifacts[0].name == "quality"
    assert (tmp_path / "registry" / "run_01__run_registry.json").exists()
    assert (tmp_path / "registry" / "run_01__run_registry.md").exists()
    index = json.loads((tmp_path / "registry" / "run_registry_index.json").read_text(encoding="utf-8"))
    assert index["runs"][0]["run_id"] == "run_01"
    assert "Use `dpone run --format json`" in report.to_markdown()


def test_run_registry_blocks_failed_or_missing_run_result(tmp_path: Path) -> None:
    failed_result = tmp_path / "failed_run.json"
    failed_result.write_text(
        json.dumps(
            {
                "run_id": "run_02",
                "process": "orders",
                "passed": False,
                "result": {"status": "failed"},
            }
        ),
        encoding="utf-8",
    )

    failed = RunRegistryService().record(output_dir=tmp_path / "failed_registry", run_result_path=failed_result)
    missing = RunRegistryService().record(
        output_dir=tmp_path / "missing_registry",
        run_result_path=tmp_path / "missing.json",
    )

    assert failed.passed is False
    assert failed.blockers == ("run_result.not_passed",)
    assert missing.passed is False
    assert missing.blockers == ("run_result.missing",)


def test_openlineage_export_builds_complete_event_from_run_registry(tmp_path: Path) -> None:
    run_entry = tmp_path / "run_01__run_registry.json"
    run_entry.write_text(
        json.dumps(
            {
                "run_id": "run_01",
                "process": "orders_daily",
                "manifest": "manifests/orders.yml",
                "status": "success",
                "passed": True,
                "run_result_sha256": "a" * 64,
                "artifacts": [{"name": "quality", "path": "quality.json", "sha256": "b" * 64, "passed": True}],
            }
        ),
        encoding="utf-8",
    )

    report = OpenLineageExportService().export(
        output_dir=tmp_path / "lineage",
        run_registry_entry_path=run_entry,
        namespace="dpone.local",
        input_datasets={"postgres": "public.orders"},
        output_datasets={"mssql": "landing.orders"},
    )

    assert report.passed is True
    assert report.event_type == "COMPLETE"
    assert report.dataset_count == 2
    assert (tmp_path / "lineage" / "run_01__openlineage.json").exists()
    event = json.loads((tmp_path / "lineage" / "run_01__openlineage.json").read_text(encoding="utf-8"))
    assert event["run"]["runId"] == "run_01"
    assert event["job"]["name"] == "orders_daily"
    assert event["inputs"] == [{"namespace": "postgres", "name": "public.orders", "facets": {}}]
    assert event["outputs"] == [{"namespace": "mssql", "name": "landing.orders", "facets": {}}]
    assert event["run"]["facets"]["dpone_run"]["status"] == "success"
    assert "Send `openlineage_event.json`" in report.to_markdown()


def test_openlineage_export_projects_complete_airflow_correlation(tmp_path: Path) -> None:
    from jsonschema import Draft202012Validator

    from dpone.contracts.airflow_correlation import build_airflow_correlation
    from dpone.contracts.airflow_run_identity import AirflowRunIdentity

    def digest(char: str) -> str:
        return "sha256:" + char * 64

    identity = AirflowRunIdentity.from_mapping(
        {
            "schema": "dpone.airflow-run-identity.v1",
            "release_id": digest("a"),
            "deployment_id": digest("b"),
            "dag_spec": {"id": "orders_daily", "sha256": digest("c")},
            "workload_pack": {"id": "load_orders", "sha256": digest("d")},
            "runtime_image_digest": digest("e"),
            "binding_set_ref": None,
            "connection_registry_ref": None,
            "credential_runtime_ref": None,
            "airflow_bundle": None,
        }
    )
    correlation = build_airflow_correlation(
        run_identity=identity,
        attempt={
            "dag_id": "orders_daily",
            "task_id": "load_orders",
            "run_id": "scheduled__2026-07-17",
            "try_number": 2,
            "map_index": 3,
        },
        dpone_run_id="run_01",
        dpone_process="orders_daily",
        runtime_evidence_sha256=digest("f"),
        pod={
            "name": "load-orders-x7f9",
            "uid": "pod-uid-123",
            "namespace": "airflow-example",
            "image_digest": digest("e"),
        },
    )
    bundle = tmp_path / "airflow-evidence-bundle.json"
    bundle.write_text(
        json.dumps({"kind": "gitops.airflow_evidence_bundle", "correlation": correlation.to_dict()}),
        encoding="utf-8",
    )
    run_entry = tmp_path / "run_01__run_registry.json"
    run_entry.write_text(
        json.dumps(
            {
                "run_id": "run_01",
                "process": "orders_daily",
                "manifest": "manifests/orders.yml",
                "status": "success",
                "passed": True,
            }
        ),
        encoding="utf-8",
    )

    report = OpenLineageExportService().export(
        output_dir=tmp_path / "lineage",
        run_registry_entry_path=run_entry,
        namespace="dpone.prod",
        airflow_evidence_bundle_path=bundle,
    )

    assert report.passed
    assert report.correlation_id == correlation.correlation_id
    event = json.loads(Path(report.event_path).read_text(encoding="utf-8"))
    assert event["run"]["runId"] == correlation.openlineage_run_id
    facet = event["run"]["facets"]["dpone_airflowCorrelation"]
    facet_schema = json.loads(
        Path("docs/schemas/openlineage/dpone-airflow-correlation-run-facet-v1.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(facet_schema).validate(facet)
    assert facet["correlationId"] == correlation.correlation_id
    assert facet["airflow"]["tryNumber"] == 2
    assert facet["artifacts"]["workloadId"] == "load_orders"
    assert facet["pod"]["uid"] == "pod-uid-123"
    assert event["run"]["facets"]["dpone_run"]["dponeRunId"] == "run_01"


def test_openlineage_export_blocks_invalid_airflow_correlation(tmp_path: Path) -> None:
    run_entry = tmp_path / "run.json"
    run_entry.write_text(json.dumps({"run_id": "run-1", "process": "orders", "passed": True}), encoding="utf-8")
    bundle = tmp_path / "bundle.json"
    bundle.write_text(json.dumps({"kind": "gitops.airflow_evidence_bundle", "correlation": {}}), encoding="utf-8")

    report = OpenLineageExportService().export(
        output_dir=tmp_path / "lineage",
        run_registry_entry_path=run_entry,
        namespace="dpone.prod",
        airflow_evidence_bundle_path=bundle,
    )

    assert not report.passed
    assert "airflow_correlation.invalid" in report.blockers


def test_openlineage_export_marks_failed_run_and_missing_registry_as_blocked(tmp_path: Path) -> None:
    failed_entry = tmp_path / "failed__run_registry.json"
    failed_entry.write_text(
        json.dumps(
            {
                "run_id": "run_02",
                "process": "orders_daily",
                "status": "failed",
                "passed": False,
            }
        ),
        encoding="utf-8",
    )

    failed = OpenLineageExportService().export(
        output_dir=tmp_path / "failed_lineage",
        run_registry_entry_path=failed_entry,
        namespace="dpone.local",
    )
    missing = OpenLineageExportService().export(
        output_dir=tmp_path / "missing_lineage",
        run_registry_entry_path=tmp_path / "missing.json",
        namespace="dpone.local",
    )

    assert failed.passed is False
    assert failed.event_type == "FAIL"
    assert failed.blockers == ("run_registry.not_passed",)
    assert missing.passed is False
    assert missing.blockers == ("run_registry.missing",)


def test_benchmark_baseline_detects_throughput_and_latency_regressions(tmp_path: Path) -> None:
    report = BenchmarkBaselineService().evaluate(
        output_dir=tmp_path / "benchmark",
        metrics={
            "throughput_rows_per_second": 85000,
            "p95_latency_seconds": 44,
            "loaded_rows": 10000,
        },
        baseline={
            "throughput_rows_per_second": {"value": 100000, "direction": "higher"},
            "p95_latency_seconds": {"value": 30, "direction": "lower"},
            "loaded_rows": {"value": 10000, "direction": "higher"},
        },
        allowed_regression_ratio=0.10,
    )

    assert report.passed is False
    assert {item.metric for item in report.items if not item.passed} == {
        "throughput_rows_per_second",
        "p95_latency_seconds",
    }
    assert (tmp_path / "benchmark" / "benchmark_baseline.json").exists()
    assert (tmp_path / "benchmark" / "benchmark_baseline.md").exists()
    assert "Re-run the same benchmark profile" in report.to_markdown()


def test_benchmark_baseline_passes_within_allowed_regression(tmp_path: Path) -> None:
    report = BenchmarkBaselineService().evaluate(
        output_dir=tmp_path / "benchmark",
        metrics={"throughput_rows_per_second": 95000, "p95_latency_seconds": 32},
        baseline={
            "throughput_rows_per_second": {"value": 100000, "direction": "higher"},
            "p95_latency_seconds": {"value": 30, "direction": "lower"},
        },
        allowed_regression_ratio=0.10,
    )

    assert report.passed is True
    assert all(item.passed for item in report.items)


def test_dbt_lineage_exports_graph_and_openlineage_events(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    run_results = tmp_path / "run_results.json"
    run_registry = tmp_path / "run_01__run_registry.json"
    manifest.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.demo.stg_orders": {
                        "unique_id": "model.demo.stg_orders",
                        "name": "stg_orders",
                        "resource_type": "model",
                        "package_name": "demo",
                        "relation_name": "analytics.stg_orders",
                        "depends_on": {"nodes": ["source.demo.raw.orders"]},
                    },
                    "model.demo.fct_orders": {
                        "unique_id": "model.demo.fct_orders",
                        "name": "fct_orders",
                        "resource_type": "model",
                        "package_name": "demo",
                        "relation_name": "analytics.fct_orders",
                        "depends_on": {"nodes": ["model.demo.stg_orders"]},
                    },
                    "test.demo.not_null_fct_orders_order_id": {
                        "unique_id": "test.demo.not_null_fct_orders_order_id",
                        "name": "not_null_fct_orders_order_id",
                        "resource_type": "test",
                        "package_name": "demo",
                        "depends_on": {"nodes": ["model.demo.fct_orders"]},
                    },
                },
                "sources": {
                    "source.demo.raw.orders": {
                        "unique_id": "source.demo.raw.orders",
                        "name": "orders",
                        "source_name": "raw",
                        "resource_type": "source",
                        "package_name": "demo",
                        "relation_name": "raw.orders",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    run_results.write_text(
        json.dumps(
            {
                "metadata": {"invocation_id": "dbt_invocation_01"},
                "results": [
                    {"unique_id": "model.demo.stg_orders", "status": "success"},
                    {"unique_id": "model.demo.fct_orders", "status": "success"},
                    {"unique_id": "test.demo.not_null_fct_orders_order_id", "status": "pass"},
                ],
            }
        ),
        encoding="utf-8",
    )
    run_registry.write_text(
        json.dumps({"run_id": "run_01", "process": "orders_dbt", "passed": True, "status": "success"}),
        encoding="utf-8",
    )

    report = DbtLineageService().export(
        output_dir=tmp_path / "dbt_lineage",
        manifest_path=manifest,
        run_results_path=run_results,
        run_registry_entry_path=run_registry,
        namespace="dbt.local",
    )

    assert report.passed is True
    assert report.run_id == "run_01"
    assert report.model_count == 2
    assert report.source_count == 1
    assert report.test_count == 1
    assert report.edge_count == 3
    graph = json.loads((tmp_path / "dbt_lineage" / "dbt_lineage.json").read_text(encoding="utf-8"))
    assert graph["nodes"]["model.demo.fct_orders"]["status"] == "success"
    assert {"upstream": "model.demo.stg_orders", "downstream": "model.demo.fct_orders"} in graph["edges"]
    events = json.loads((tmp_path / "dbt_lineage" / "dbt_openlineage.json").read_text(encoding="utf-8"))["events"]
    assert len(events) == 2
    fct_event = next(event for event in events if event["job"]["name"] == "dbt.fct_orders")
    assert fct_event["inputs"] == [{"namespace": "dbt.local", "name": "analytics.stg_orders", "facets": {}}]
    assert fct_event["outputs"] == [{"namespace": "dbt.local", "name": "analytics.fct_orders", "facets": {}}]
    assert "Send `dbt_openlineage.json`" in report.to_markdown()


def test_dbt_lineage_blocks_missing_manifest_and_failed_dbt_results(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    run_results = tmp_path / "run_results.json"
    manifest.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.demo.fct_orders": {
                        "unique_id": "model.demo.fct_orders",
                        "name": "fct_orders",
                        "resource_type": "model",
                        "relation_name": "analytics.fct_orders",
                        "depends_on": {"nodes": []},
                    }
                },
                "sources": {},
            }
        ),
        encoding="utf-8",
    )
    run_results.write_text(
        json.dumps({"results": [{"unique_id": "model.demo.fct_orders", "status": "error"}]}),
        encoding="utf-8",
    )

    failed = DbtLineageService().export(
        output_dir=tmp_path / "failed",
        manifest_path=manifest,
        run_results_path=run_results,
    )
    missing = DbtLineageService().export(
        output_dir=tmp_path / "missing",
        manifest_path=tmp_path / "missing_manifest.json",
    )

    assert failed.passed is False
    assert failed.blockers == ("dbt_results.not_passed",)
    assert missing.passed is False
    assert missing.blockers == ("dbt_manifest.missing",)


def test_certification_suite_aggregates_matrix_benchmark_lineage_and_evidence(tmp_path: Path) -> None:
    certification = tmp_path / "certification_report.json"
    benchmark = tmp_path / "benchmark_baseline.json"
    lineage = tmp_path / "openlineage_report.json"
    dbt_lineage = tmp_path / "dbt_lineage_report.json"
    evidence = tmp_path / "evidence_bundle.json"
    strategy_certification = tmp_path / "strategy_certification_bundle.json"
    certification.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "PASS",
                "production_certification": "VERIFIED",
                "results": [
                    {"case_id": "postgres_to_mssql__incremental_merge", "passed": True},
                    {"case_id": "mssql_to_clickhouse__replace", "passed": True},
                ],
            }
        ),
        encoding="utf-8",
    )
    benchmark.write_text(json.dumps({"passed": True, "items": []}), encoding="utf-8")
    lineage.write_text(json.dumps({"passed": True, "event_type": "COMPLETE"}), encoding="utf-8")
    dbt_lineage.write_text(json.dumps({"passed": True, "model_count": 4}), encoding="utf-8")
    evidence.write_text(json.dumps({"passed": True, "items": []}), encoding="utf-8")
    strategy_certification.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "PASS",
                "production_certification": "VERIFIED",
                "schema_version": "dpone.strategy.certification_bundle.v1",
                "summary": {"total_items": 3, "passed_items": 3},
                "items": [],
            }
        ),
        encoding="utf-8",
    )

    report = CertificationSuiteService().evaluate(
        output_dir=tmp_path / "suite",
        suite_id="oss_mvp_manual",
        certification_report_path=certification,
        benchmark_baseline_path=benchmark,
        lineage_report_path=lineage,
        dbt_lineage_report_path=dbt_lineage,
        evidence_bundle_path=evidence,
        strategy_certification_bundle_path=strategy_certification,
        require_benchmark=True,
        require_lineage=True,
        require_dbt_lineage=True,
        require_evidence=True,
        require_strategy_certification=True,
    )

    assert report.passed is True
    assert report.suite_id == "oss_mvp_manual"
    assert report.case_count == 2
    assert report.evidence_count == 6
    assert {item.name for item in report.items} == {
        "certification_report",
        "benchmark_baseline",
        "lineage_report",
        "dbt_lineage_report",
        "evidence_bundle",
        "strategy_certification_bundle",
    }
    assert all(len(item.sha256) == 64 for item in report.items)
    assert (tmp_path / "suite" / "certification_suite.json").exists()
    assert (tmp_path / "suite" / "certification_suite.md").exists()
    assert (tmp_path / "suite" / "certification_suite_index.json").exists()
    assert "Run the same certification profile again" in report.to_markdown()


def test_release_bound_certification_suite_requires_matching_child_release_identity(
    tmp_path: Path,
) -> None:
    release = "sha256:" + "a" * 64
    certification = tmp_path / "certification_report.json"
    certification.write_text(
        json.dumps(
            {
                "release_id": release,
                "passed": True,
                "evidence_status": "PASS",
                "production_certification": "VERIFIED",
                "blockers": [],
                "results": [{"case_id": "postgres_to_mssql__replace", "passed": True}],
            }
        ),
        encoding="utf-8",
    )

    report = CertificationSuiteService().evaluate(
        output_dir=tmp_path / "suite",
        suite_id="release_bound",
        release_id=release,
        certification_report_path=certification,
    )

    assert report.passed is True
    assert report.release_id == release
    assert report.items[0].identity_status == "MATCHED"


def test_release_bound_certification_suite_blocks_cross_release_replay(
    tmp_path: Path,
) -> None:
    certification = tmp_path / "certification_report.json"
    certification.write_text(
        json.dumps(
            {
                "release_id": "sha256:" + "a" * 64,
                "passed": True,
                "evidence_status": "PASS",
                "blockers": [],
                "results": [{"case_id": "postgres_to_mssql__replace", "passed": True}],
            }
        ),
        encoding="utf-8",
    )

    report = CertificationSuiteService().evaluate(
        output_dir=tmp_path / "suite",
        suite_id="replayed",
        release_id="sha256:" + "b" * 64,
        certification_report_path=certification,
    )

    assert report.passed is False
    assert report.blockers == ("certification_report.release_id_mismatch",)
    assert report.items[0].identity_status == "MISMATCHED"


def test_release_bound_certification_suite_blocks_unbound_child_artifact(
    tmp_path: Path,
) -> None:
    certification = tmp_path / "certification_report.json"
    certification.write_text(
        json.dumps({"passed": True, "evidence_status": "PASS", "blockers": [], "results": []}),
        encoding="utf-8",
    )

    report = CertificationSuiteService().evaluate(
        output_dir=tmp_path / "suite",
        suite_id="unbound",
        release_id="sha256:" + "b" * 64,
        certification_report_path=certification,
    )

    assert report.passed is False
    assert report.blockers == ("certification_report.release_id_missing",)
    assert report.items[0].identity_status == "MISSING"


def test_certification_suite_rejects_mock_contract_report(tmp_path: Path) -> None:
    CertificationHarnessService().run_mock_contract(
        artifact_dir=tmp_path,
        source="postgres",
        sink="mssql",
        strategy="snapshot_diff",
    )

    report = CertificationSuiteService().evaluate(
        output_dir=tmp_path / "suite",
        suite_id="mock_must_not_certify",
        certification_report_path=tmp_path / "certification_report.json",
    )

    assert report.passed is False
    assert report.evidence_status == "UNVERIFIED"
    assert report.blockers == ("certification_report.not_passed",)


def test_certification_suite_blocks_failed_and_missing_required_artifacts(tmp_path: Path) -> None:
    certification = tmp_path / "certification_report.json"
    certification.write_text(
        json.dumps(
            {
                "passed": False,
                "results": [{"case_id": "postgres_to_mssql__incremental_merge", "passed": False}],
            }
        ),
        encoding="utf-8",
    )

    report = CertificationSuiteService().evaluate(
        output_dir=tmp_path / "suite",
        suite_id="red_suite",
        certification_report_path=certification,
        benchmark_baseline_path=tmp_path / "missing_benchmark.json",
        strategy_certification_bundle_path=tmp_path / "missing_strategy_bundle.json",
        require_benchmark=True,
        require_strategy_certification=True,
    )

    assert report.passed is False
    assert report.blockers == (
        "certification_report.not_passed",
        "benchmark_baseline.missing",
        "strategy_certification_bundle.missing",
    )


def test_integration_matrix_report_aggregates_case_and_behavior_artifacts(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "matrix"
    artifact_dir.mkdir()
    (artifact_dir / "postgres_to_mssql__incremental_merge.json").write_text(
        json.dumps(
            {
                "case_id": "postgres_to_mssql__incremental_merge",
                "source": "postgres",
                "sink": "mssql",
                "strategy": "incremental_merge",
                "guide": "source-sink/postgres-to-mssql.md",
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "postgres_to_mssql__incremental_merge__behavior.json").write_text(
        json.dumps(
            {
                "case_id": "postgres_to_mssql__incremental_merge",
                "source": "postgres",
                "sink": "mssql",
                "strategy": "incremental_merge",
                "passed": True,
                "expected_row_count": 10000,
                "actual_row_count": 10000,
                "quality_checks": ["delete_keys_absent"],
            }
        ),
        encoding="utf-8",
    )

    report = IntegrationMatrixReportService().build(artifact_dir=artifact_dir, output_dir=tmp_path / "report")

    assert report.passed is True
    assert report.total_cases == 1
    assert report.passed_cases == 1
    assert report.failed_cases == 0
    assert report.results[0].case_id == "postgres_to_mssql__incremental_merge"
    assert report.results[0].behavior_present is True
    assert report.results[0].quality_checks == ("delete_keys_absent",)
    payload = json.loads((tmp_path / "report" / "certification_report.json").read_text(encoding="utf-8"))
    assert payload["passed"] is True
    assert payload["results"][0]["passed"] is True
    assert (tmp_path / "report" / "certification_report.md").exists()


def test_integration_matrix_report_blocks_missing_behavior_artifacts(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "matrix"
    artifact_dir.mkdir()
    (artifact_dir / "postgres_to_mssql__replace.json").write_text(
        json.dumps(
            {
                "case_id": "postgres_to_mssql__replace",
                "source": "postgres",
                "sink": "mssql",
                "strategy": "replace",
            }
        ),
        encoding="utf-8",
    )

    report = IntegrationMatrixReportService().build(artifact_dir=artifact_dir, output_dir=tmp_path / "report")

    assert report.passed is False
    assert report.blockers == ("matrix_behavior.missing:postgres_to_mssql__replace",)


def test_artifact_index_classifies_strategy_and_suite_evidence(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    (root / "strategy_certification_bundle.json").write_text(json.dumps({"passed": True}), encoding="utf-8")
    (root / "certification_suite.json").write_text(json.dumps({"passed": True}), encoding="utf-8")
    (root / "matrix_123__evidence_chain.json").write_text(json.dumps({"verified": True}), encoding="utf-8")

    report = ArtifactIndexService().build(output_dir=tmp_path / "index", roots=[root], release="matrix_123")

    artifact_types = {item.name: item.artifact_type for item in report.items}
    assert artifact_types["strategy_certification_bundle.json"] == "strategy_certification_bundle"
    assert artifact_types["certification_suite.json"] == "certification_suite"
    assert artifact_types["matrix_123__evidence_chain.json"] == "evidence_chain"
