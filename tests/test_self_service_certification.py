from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator

from dpone.cli import main as cli_main
from dpone.contracts.airflow_correlation import build_airflow_correlation
from dpone.contracts.airflow_deployment import deployment_id, release_id
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.route_attestation import RouteAttestationExpectedSubject, route_attestation_id, sha256_bytes
from dpone.ops.route_certification_matrix_evidence import RouteCertificationEvidenceReader
from dpone.ops.self_service_certification import (
    SelfServiceCertificationError,
    SelfServiceCertificationRequest,
    SelfServiceCertificationService,
)
from dpone.ops.self_service_certification_models import UsabilityThresholds
from dpone.ops.self_service_certification_reference import ReferenceDeploymentEvidenceReader
from dpone.ops.self_service_certification_usability import SelfServiceUsabilityReader

_COMMIT = "17e61ed053c9853b8aec9ac5fe936c3bba46bb50"
_NOW = datetime(2026, 7, 16, 12, tzinfo=UTC)
_ROUTE_ID = "mssql_clickhouse_incremental_merge_airflow_kpo"
_ROUTE = {
    "route_id": _ROUTE_ID,
    "source": "mssql",
    "sink": "clickhouse",
    "strategy": "incremental_merge",
    "transport": "native_bcp_to_clickhouse",
    "schema_evolution": "widening",
    "airflow_runtime_mode": "kpo",
    "sampling_mode": "pushdown",
}


def test_missing_external_evidence_is_honestly_unverified_and_schema_valid(tmp_path: Path) -> None:
    report = _service().publish(_request(tmp_path))

    assert report.overall_status == "UNVERIFIED"
    assert report.has_input_failures is False
    assert report.usability.status == "UNVERIFIED"
    assert report.reference_deployments.status == "UNVERIFIED"
    payload = json.loads(report.json_path.read_text(encoding="utf-8"))
    schema = json.loads(Path("docs/schemas/gitops/self-service-certification.schema.json").read_text())
    Draft202012Validator(schema).validate(payload)


def test_five_fast_unassisted_first_time_users_pass_without_leaking_identity(tmp_path: Path) -> None:
    study = _write_study(tmp_path / "study.json", sessions=5)
    report = _service().publish(_request(tmp_path, usability=study))

    assert report.usability.status == "PASS"
    assert report.usability.complete_success_rate == 1.0
    assert report.overall_status == "UNVERIFIED"
    output = report.json_path.read_text(encoding="utf-8")
    assert "participant_ref" not in output
    assert "session-01" not in output
    assert "transcript_sha256" not in output


def test_certification_v1_threshold_payload_remains_schema_compatible() -> None:
    assert UsabilityThresholds().to_dict() == {
        "minimum_participants": 5,
        "minimum_success_rate": 0.8,
        "maximum_first_dag_seconds": 600,
        "maximum_safe_sample_seconds": 900,
        "maximum_commands": 5,
    }


@pytest.mark.parametrize("commands_used", (0, 1, 4, 6))
def test_v1_study_requires_its_exact_five_command_protocol(
    commands_used: int,
    tmp_path: Path,
) -> None:
    study = _write_study(tmp_path / "study.json", sessions=5, commands_used=commands_used)
    reader = SelfServiceUsabilityReader()

    evidence = reader.read(study, expected_commit=_COMMIT)

    assert evidence.status == "FAIL"
    assert evidence.successful_sessions == 0
    assert "self_service.usability_complete_success_below_target" in evidence.blockers


def test_four_valid_users_remain_unverified(tmp_path: Path) -> None:
    report = _service().publish(_request(tmp_path, usability=_write_study(tmp_path / "study.json", sessions=4)))

    assert report.usability.status == "UNVERIFIED"
    assert report.has_input_failures is False
    assert "self_service.usability_participants_insufficient" in report.usability.blockers


def test_completed_study_below_target_fails(tmp_path: Path) -> None:
    study = _write_study(tmp_path / "study.json", sessions=5, assistance_for={0, 1})
    report = _service().publish(_request(tmp_path, usability=study))

    assert report.usability.status == "FAIL"
    assert report.has_input_failures is True
    assert "self_service.usability_complete_success_below_target" in report.usability.blockers


def test_blocked_sessions_cannot_produce_pass_evidence(tmp_path: Path) -> None:
    study = _write_study(tmp_path / "study.json", sessions=5)
    payload = json.loads(study.read_text())
    for session in payload["sessions"]:
        session["blocker_codes"] = ["safe_sample_failed"]
    _write_json(study, payload)

    report = _service().publish(_request(tmp_path, usability=study))

    assert report.usability.status == "FAIL"
    assert report.usability.successful_sessions == 0
    assert report.usability.complete_success_rate == 0.0
    assert report.has_input_failures is True
    assert "self_service.usability_complete_success_below_target" in report.usability.blockers


def test_unknown_privacy_field_fails_closed(tmp_path: Path) -> None:
    study = _write_study(tmp_path / "study.json", sessions=5)
    payload = json.loads(study.read_text())
    payload["sessions"][0]["email"] = "person@example.test"
    _write_json(study, payload)

    report = _service().publish(_request(tmp_path, usability=study))

    assert report.usability.status == "FAIL"
    assert report.usability.valid_sessions == 0


def test_symlinked_study_is_usage_error(tmp_path: Path) -> None:
    source = _write_study(tmp_path / "source.json", sessions=5)
    linked = tmp_path / "study.json"
    linked.symlink_to(source)

    with pytest.raises(SelfServiceCertificationError) as exc:
        _service().publish(_request(tmp_path, usability=linked))

    assert exc.value.code == "DPONE_SELF_SERVICE_STUDY_INPUT_UNSAFE"


def test_two_independent_production_references_pass_reference_gate(tmp_path: Path) -> None:
    first = _write_reference(tmp_path / "prod-a", marker="a", signer="ci://org-a")
    second = _write_reference(tmp_path / "prod-b", marker="b", signer="ci://org-b")
    report = _service().publish(_request(tmp_path, references=(first, second)))

    assert report.reference_deployments.status == "PASS"
    assert report.reference_deployments.valid_count == 2
    assert report.reference_deployments.distinct_deployment_count == 2
    assert report.reference_deployments.distinct_signer_count == 2
    assert report.overall_status == "UNVERIFIED"


def test_same_signer_cannot_satisfy_independence(tmp_path: Path) -> None:
    first = _write_reference(tmp_path / "prod-a", marker="a", signer="ci://same")
    second = _write_reference(tmp_path / "prod-b", marker="b", signer="ci://same")
    report = _service().publish(_request(tmp_path, references=(first, second)))

    assert report.reference_deployments.status == "UNVERIFIED"
    assert "self_service.reference_signers_not_independent" in report.reference_deployments.blockers


def test_tampered_deployment_makes_supplied_reference_fail(tmp_path: Path) -> None:
    reference = _write_reference(tmp_path / "prod-a", marker="a", signer="ci://org-a")
    deployment_path = reference / "deployment-set.json"
    payload = json.loads(deployment_path.read_text())
    payload["runtime_image_digest"] = "sha256:" + "0" * 64
    _write_json(deployment_path, payload)

    report = _service().publish(_request(tmp_path, references=(reference,)))

    assert report.reference_deployments.status == "FAIL"
    assert report.has_input_failures is True
    assert "self_service.reference_deployment_digest_mismatch" in report.reference_deployments.blockers


@pytest.mark.parametrize(
    "case",
    (
        "current",
        "latest_segment",
        "mixed_case_alias",
        "percent_encoded_alias",
        "missing_top_level",
        "empty_source",
        "source_mismatch",
    ),
)
def test_reference_certification_rejects_unpinned_or_mismatched_delivery(
    tmp_path: Path,
    case: str,
) -> None:
    signer = "ci://org-a"
    reference = _write_reference(tmp_path / "prod-a", marker="a", signer=signer)
    _rewrite_reference_delivery(reference, case=case, signer=signer)

    report = _service().publish(_request(tmp_path, references=(reference,)))

    assert report.reference_deployments.status == "FAIL"
    assert report.has_input_failures is True
    assert "self_service.reference_delivery_invalid" in report.reference_deployments.blockers


def test_airflow_attempt_mismatch_makes_reference_fail(tmp_path: Path) -> None:
    reference = _write_reference(tmp_path / "prod-a", marker="a", signer="ci://org-a")
    airflow_path = reference / "airflow-evidence-bundle.json"
    payload = json.loads(airflow_path.read_text())
    payload["attempt"]["run_id"] = "different"
    _write_json(airflow_path, payload)

    report = _service().publish(_request(tmp_path, references=(reference,)))

    assert report.reference_deployments.status == "FAIL"
    assert "self_service.reference_cross_identity_mismatch" in report.reference_deployments.blockers


def test_overall_pass_requires_both_real_evidence_axes(tmp_path: Path) -> None:
    study = _write_study(tmp_path / "study.json", sessions=5)
    references = (
        _write_reference(tmp_path / "prod-a", marker="a", signer="ci://org-a"),
        _write_reference(tmp_path / "prod-b", marker="b", signer="ci://org-b"),
    )
    report = _service().publish(_request(tmp_path, usability=study, references=references))

    assert report.overall_status == "PASS"
    assert report.has_input_failures is False


def test_exact_republication_is_noop_and_changed_input_conflicts(tmp_path: Path) -> None:
    request = _request(tmp_path)
    first = _service().publish(request)
    second = SelfServiceCertificationService(clock=lambda: _NOW + timedelta(minutes=5)).publish(request)
    assert first.to_dict() == second.to_dict()
    first.markdown_path.write_text("changed\n", encoding="utf-8")

    with pytest.raises(SelfServiceCertificationError) as exc:
        _service().publish(request)

    assert exc.value.code == "DPONE_SELF_SERVICE_OUTPUT_CONFLICT"


def test_cli_no_evidence_returns_nonzero_with_unverified_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _stub_cli(monkeypatch)
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "certify",
                "self-service",
                "--commit-sha",
                _COMMIT,
                "--output-dir",
                str(tmp_path / "out"),
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 1
    assert json.loads(capsys.readouterr().out)["overall_status"] == "UNVERIFIED"


def test_cli_allow_unverified_returns_zero_for_publish_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _stub_cli(monkeypatch)
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "certify",
                "self-service",
                "--commit-sha",
                _COMMIT,
                "--output-dir",
                str(tmp_path / "out"),
                "--format",
                "json",
                "--allow-unverified",
            ]
        )
    assert exc.value.code == 0
    assert json.loads(capsys.readouterr().out)["overall_status"] == "UNVERIFIED"


def test_cli_bad_commit_returns_structured_usage_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _stub_cli(monkeypatch)
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "certify",
                "self-service",
                "--commit-sha",
                "bad",
                "--output-dir",
                str(tmp_path / "out"),
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 2
    assert json.loads(capsys.readouterr().out)["code"] == "DPONE_SELF_SERVICE_COMMIT_INVALID"


def _accepting_crypto_gate(
    *,
    directory: Path,
    attestation_path: Path,
    certification_bundle_path: Path,
    expected: RouteAttestationExpectedSubject,
) -> str | None:
    del directory, attestation_path, certification_bundle_path, expected
    return None


def _service() -> SelfServiceCertificationService:
    route_reader = RouteCertificationEvidenceReader(crypto_gate=_accepting_crypto_gate)
    return SelfServiceCertificationService(
        reference_reader=ReferenceDeploymentEvidenceReader(route_reader=route_reader),
        clock=lambda: _NOW,
    )


def _request(
    root: Path,
    *,
    usability: Path | None = None,
    references: tuple[Path, ...] = (),
) -> SelfServiceCertificationRequest:
    return SelfServiceCertificationRequest(
        expected_commit=_COMMIT,
        usability_study=usability,
        reference_deployments=references,
        output_dir=root / "out",
    )


def _write_study(
    path: Path,
    *,
    sessions: int,
    assistance_for: set[int] | None = None,
    commands_used: int = 5,
) -> Path:
    start = datetime(2026, 7, 16, 9, tzinfo=UTC)
    assistance_for = assistance_for or set()
    rows = []
    for index in range(sessions):
        session_start = start + timedelta(minutes=20 * index)
        rows.append(
            {
                "session_id": f"session-{index + 1:02d}",
                "participant_ref": "sha256:" + f"{index + 1:064x}",
                "first_time_dpone_user": True,
                "consent_recorded": True,
                "started_at": _time(session_start),
                "dag_preview_at": _time(session_start + timedelta(minutes=7)),
                "safe_sample_at": _time(session_start + timedelta(minutes=12)),
                "finished_at": _time(session_start + timedelta(minutes=12)),
                "outcome": "passed",
                "commands_used": commands_used,
                "assistance_events": 1 if index in assistance_for else 0,
                "authored_airflow_python": False,
                "transcript_sha256": "sha256:" + f"{index + 101:064x}",
                "blocker_codes": [],
            }
        )
    return _write_json(
        path,
        {
            "schema": "dpone.self-service-usability-study.v1",
            "protocol": "airflow_first_dag_and_safe_sample_v1",
            "target_commit": _COMMIT,
            "facilitator_ref": "research-team-1",
            "started_at": _time(start),
            "completed_at": _time(start + timedelta(hours=4)),
            "sessions": rows,
        },
    )


def _write_reference(root: Path, *, marker: str, signer: str) -> Path:
    root.mkdir()
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {"dag_specs": [], "workload_packs": [], "canonical_schemas": []},
        "provenance": {"source_commit": _COMMIT, "build_id": "pytest", "built_at": _time(_NOW)},
    }
    release["release_id"] = release_id(release)
    _write_json(root / "release-set.json", release)
    deployment = _deployment(str(release["release_id"]), marker=marker)
    _write_json(root / "deployment-set.json", deployment)
    live_stage_path = root / "route_live_certification.json"
    _write_json(
        live_stage_path,
        {
            "schema_version": "dpone.route_live_certification.v1",
            "release_id": release["release_id"],
            "passed": True,
            "evidence_status": "PASS",
            "blockers": [],
        },
    )
    bundle_path = root / "route_certification_bundle.json"
    _write_json(
        bundle_path,
        _route_bundle(
            str(release["release_id"]),
            stage_path=live_stage_path.name,
            stage_sha256=sha256_bytes(live_stage_path.read_bytes()).removeprefix("sha256:"),
        ),
    )
    bundle_bytes = bundle_path.read_bytes()
    _write_route_attestation(
        root, release=str(release["release_id"]), deployment=deployment, bundle=bundle_bytes, signer=signer
    )
    _write_json(root / "airflow-evidence-bundle.json", _airflow_evidence(deployment))
    return root


def _rewrite_reference_delivery(root: Path, *, case: str, signer: str) -> None:
    release = json.loads((root / "release-set.json").read_text(encoding="utf-8"))
    deployment_path = root / "deployment-set.json"
    deployment = json.loads(deployment_path.read_text(encoding="utf-8"))
    delivery = deployment["runtime_artifact_delivery"]
    if case == "current":
        delivery["artifact_registry_ref"] = "CURRENT"
        delivery["source"]["artifact_registry_ref"] = "CURRENT"
    elif case == "latest_segment":
        delivery["artifact_registry_ref"] = "registry/releases/latest"
        delivery["source"]["artifact_registry_ref"] = "registry/releases/latest"
    elif case == "mixed_case_alias":
        delivery["artifact_registry_ref"] = "registry/CurRent/release"
        delivery["source"]["artifact_registry_ref"] = "registry/CurRent/release"
    elif case == "percent_encoded_alias":
        delivery["artifact_registry_ref"] = "registry/%63urrent/release"
        delivery["source"]["artifact_registry_ref"] = "registry/%63urrent/release"
    elif case == "missing_top_level":
        delivery.pop("artifact_registry_ref")
    elif case == "empty_source":
        delivery["source"]["artifact_registry_ref"] = ""
    elif case == "source_mismatch":
        delivery["source"]["artifact_registry_ref"] = "other-prod-artifacts"
    else:  # pragma: no cover - parametrization is exhaustive.
        raise AssertionError(f"unsupported case: {case}")
    deployment["deployment_id"] = ""
    deployment["deployment_id"] = deployment_id(deployment)
    _write_json(deployment_path, deployment)
    bundle_path = root / "route_certification_bundle.json"
    _write_route_attestation(
        root,
        release=str(release["release_id"]),
        deployment=deployment,
        bundle=bundle_path.read_bytes(),
        signer=signer,
    )
    _write_json(root / "airflow-evidence-bundle.json", _airflow_evidence(deployment))


def _deployment(release: str, *, marker: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": "",
        "deployment_type": "environment",
        "runnable": True,
        "environment": "production",
        "release_ref": release,
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "runtime_image_digest": "sha256:" + marker * 64,
        "runtime_artifact_delivery": {
            "mode": "init_fetch",
            "artifact_registry_ref": "dpone-prod-artifacts",
            "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
            "source": {"artifact_registry_ref": "dpone-prod-artifacts"},
            "verify": {"checksums": "required", "attestations": "required_for_prod"},
        },
    }
    payload["deployment_id"] = deployment_id(payload)
    return payload


def _route_bundle(
    release: str,
    *,
    stage_path: str,
    stage_sha256: str,
) -> dict[str, object]:
    return {
        "schema_version": "dpone.route_certification_bundle.v1",
        "evidence_status": "PASS",
        "release": "v0.72.3",
        "profile": "vendor_live",
        "route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"},
        "passed": True,
        "level": "certified",
        "blockers": [],
        "required_evidence": ["route_live_evidence_bundle"],
        "matrix_claim": {
            "schema": "dpone.route-matrix-claim.v1",
            "release_id": release,
            "source_commit": _COMMIT,
            "certified_at": _time(_NOW),
            "route": dict(_ROUTE),
        },
        "stages": [
            {
                "name": "route_live_evidence_bundle",
                "path": stage_path,
                "sha256": stage_sha256,
                "passed": True,
                "required": True,
                "summary": "approved live route evidence",
                "blockers": [],
            }
        ],
        "artifact_index": {"route_live_evidence_bundle": stage_path},
    }


def _write_route_attestation(
    root: Path,
    *,
    release: str,
    deployment: dict[str, object],
    bundle: bytes,
    signer: str,
) -> None:
    claims = {
        "route": dict(_ROUTE),
        "certification": {"bundle_sha256": sha256_bytes(bundle), "profile": "vendor_live", "level": "certified"},
        "subject": {
            "release_id": release,
            "deployment_id": deployment["deployment_id"],
            "environment": "production",
            "runtime_image_digest": deployment["runtime_image_digest"],
            "authorization_profile": "safe_sample_production",
        },
        "validity": {
            "issued_at": _time(_NOW - timedelta(hours=1)),
            "not_before": _time(_NOW - timedelta(hours=1)),
            "expires_at": _time(_NOW + timedelta(days=1)),
        },
    }
    attestation = {
        "schema": "dpone.route-attestation.v1",
        "attestation_id": route_attestation_id(claims),
        "claims": claims,
    }
    attestation_path = root / "route-attestation.json"
    _write_json(attestation_path, attestation)
    attestation_bytes = attestation_path.read_bytes()
    _write_json(
        root / "route-attestation-verification.json",
        {
            "schema": "dpone.route-attestation-verification.v1",
            "decision": "verified",
            "code": "DPONE_ROUTE_ATTESTATION_VERIFIED",
            "message": "verified",
            "attestation_id": attestation["attestation_id"],
            "attestation_sha256": sha256_bytes(attestation_bytes),
            "certification_bundle_sha256": sha256_bytes(bundle),
            "policy_fingerprint": "sha256:" + "e" * 64,
            "route_id": _ROUTE_ID,
            "release_id": release,
            "deployment_id": deployment["deployment_id"],
            "environment": "production",
            "authorization_profile": "safe_sample_production",
            "signer": {
                "backend": "cosign_keyless_v1",
                "certificate_identity": signer,
                "certificate_oidc_issuer": "https://token.actions.githubusercontent.com",
                "verifier_version": "3.0.4",
            },
            "validity": {
                "not_before": claims["validity"]["not_before"],
                "expires_at": claims["validity"]["expires_at"],
            },
            "verified_at": _time(_NOW),
            "errors": [],
        },
    )


def _airflow_evidence(deployment: dict[str, object]) -> dict[str, object]:
    run_identity = AirflowRunIdentity.from_mapping(
        {
            "schema": "dpone.airflow-run-identity.v1",
            "release_id": deployment["release_ref"],
            "deployment_id": deployment["deployment_id"],
            "dag_spec": {"id": "orders_daily", "sha256": "sha256:" + "4" * 64},
            "workload_pack": {"id": "load_orders", "sha256": "sha256:" + "5" * 64},
            "runtime_image_digest": deployment["runtime_image_digest"],
            "binding_set_ref": deployment["binding_set_ref"],
            "connection_registry_ref": deployment["connection_registry_ref"],
            "credential_runtime_ref": deployment["credential_runtime_ref"],
            "airflow_bundle": {
                "backend": "git",
                "ref": "git:7ac31f2",
                "versioned": True,
                "version": "7ac31f2",
                "snapshot_ref": None,
            },
        }
    )
    attempt = {
        "dag_id": "orders_daily",
        "task_id": "load_orders",
        "run_id": "manual__1",
        "try_number": 1,
        "map_index": -1,
    }
    pod = {
        "name": "dpone-runtime",
        "uid": "pod-uid",
        "namespace": "airflow-example",
        "image_digest": deployment["runtime_image_digest"],
    }
    correlation = build_airflow_correlation(
        run_identity=run_identity,
        attempt=attempt,
        dpone_run_id="orders-run-1",
        dpone_process="orders",
        runtime_evidence_sha256="sha256:" + "6" * 64,
        pod=pod,
    )
    return {
        "kind": "gitops.airflow_evidence_bundle",
        "schema_version": "1",
        "producer": "pytest",
        "runner_policy": "release",
        "attempt": attempt,
        "pod": {
            "pod_name": "dpone-runtime",
            "pod_uid": "pod-uid",
            "namespace": "airflow-example",
            "service_account": "dpone-runtime",
            "image": "ghcr.io/acme/dpone@" + str(deployment["runtime_image_digest"]),
            "image_digest": deployment["runtime_image_digest"],
        },
        "artifacts": [{"name": "runtime_evidence", "required": True, "exists": True, "passed": True}],
        "warnings": [],
        "blockers": [],
        "run_identity": run_identity.to_dict(),
        "correlation": correlation.to_dict(),
    }


def _write_json(path: Path, payload: object) -> Path:
    data = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    path.write_bytes(data)
    return path


def _time(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _stub_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = SimpleNamespace(info=lambda *args: None, error=lambda *args: None)
    monkeypatch.setattr(cli_main, "setup_logging", lambda: logger)
    monkeypatch.setattr(
        cli_main.AppContext,
        "from_env",
        staticmethod(lambda *, logger: SimpleNamespace(logger=logger)),
    )
