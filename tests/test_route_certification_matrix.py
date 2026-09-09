from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator

from dpone.cli import main as cli_main
from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.route_attestation import RouteAttestationExpectedSubject, route_attestation_id, sha256_bytes
from dpone.ops.route_certification_matrix import (
    RouteCertificationMatrixError,
    RouteCertificationMatrixRequest,
    RouteCertificationMatrixService,
)
from dpone.ops.route_certification_matrix_evidence import RouteCertificationEvidenceReader

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


def test_catalog_only_matrix_is_honestly_experimental_and_schema_valid(tmp_path: Path) -> None:
    report = _service().publish(_request(tmp_path))

    assert report.has_input_failures is False
    assert report.counts == {
        "experimental": 1,
        "route-certified": 0,
        "production-certified": 0,
        "enterprise-certified": 0,
    }
    row = report.rows[0]
    assert row.route_id == _ROUTE_ID
    assert row.status == "experimental"
    assert row.contract_status == "UNVERIFIED"
    assert row.live_status == "UNVERIFIED"
    assert row.production_status == "UNVERIFIED"
    assert row.dimensions.to_dict() == {
        key: _ROUTE[key]
        for key in (
            "source",
            "sink",
            "strategy",
            "transport",
            "schema_evolution",
            "airflow_runtime_mode",
        )
    }
    payload = json.loads((tmp_path / "out/route-certification-matrix.json").read_text())
    schema = json.loads(Path("docs/schemas/gitops/route-certification-matrix.schema.json").read_text())
    Draft202012Validator(schema).validate(payload)


def test_exact_vendor_live_bundle_promotes_only_to_route_certified(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a")

    report = _service().publish(_request(tmp_path, evidence))

    row = report.rows[0]
    assert report.has_input_failures is False
    assert row.status == "route-certified"
    assert row.contract_status == "PASS"
    assert row.live_status == "PASS"
    assert row.production_status == "UNVERIFIED"
    assert row.proofs[0].release_id.startswith("sha256:")
    assert row.proofs[0].certification_bundle_sha256.startswith("sha256:")


def test_verified_production_attestation_promotes_to_production_certified(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a", deployment="a", signer="ci://org-a")

    report = _service().publish(_request(tmp_path, evidence))

    row = report.rows[0]
    assert row.status == "production-certified"
    assert row.production_status == "PASS"
    assert row.proofs[0].deployment_id == "sha256:" + "a" * 64
    assert row.proofs[0].signer_identity == "ci://org-a"


def test_consistency_only_verification_receipt_cannot_promote_without_crypto(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a", deployment="a", signer="ci://org-a")

    report = RouteCertificationMatrixService(clock=lambda: _NOW).publish(_request(tmp_path, evidence))

    row = report.rows[0]
    assert row.status == "experimental"
    assert "route_matrix.attestation_crypto_proof_missing" in row.blockers
    assert row.production_status != "PASS"


def test_two_independent_production_proofs_promote_to_enterprise_certified(tmp_path: Path) -> None:
    first = _write_evidence_set(tmp_path / "evidence-a", deployment="a", signer="ci://org-a")
    second = _write_evidence_set(tmp_path / "evidence-b", deployment="b", signer="ci://org-b")

    report = _service().publish(_request(tmp_path, first, second))

    assert report.rows[0].status == "enterprise-certified"
    assert report.counts["enterprise-certified"] == 1


def test_same_signer_is_not_independent_enterprise_evidence(tmp_path: Path) -> None:
    first = _write_evidence_set(tmp_path / "evidence-a", deployment="a", signer="ci://same")
    second = _write_evidence_set(tmp_path / "evidence-b", deployment="b", signer="ci://same")

    report = _service().publish(_request(tmp_path, first, second))

    assert report.rows[0].status == "production-certified"
    assert "route_matrix.enterprise_independence_missing" in report.rows[0].blockers


def test_wrong_commit_fails_supplied_evidence_without_hiding_matrix(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a", source_commit="different")

    report = _service().publish(_request(tmp_path, evidence))

    assert report.has_input_failures is True
    assert report.rows[0].status == "experimental"
    assert report.rows[0].contract_status == "FAIL"
    assert "route_matrix.release_commit_mismatch" in report.rows[0].blockers


def test_stale_bundle_cannot_promote_route(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a")
    bundle_path = evidence / "route_certification_bundle.json"
    bundle = json.loads(bundle_path.read_text())
    bundle["matrix_claim"]["certified_at"] = (_NOW - timedelta(days=30)).isoformat()
    _write_json(bundle_path, bundle)

    report = _service().publish(_request(tmp_path, evidence))

    assert report.has_input_failures is True
    assert report.rows[0].status == "experimental"
    assert "route_matrix.certification_bundle_stale" in report.rows[0].blockers


def test_bundle_stage_must_exist_match_digest_and_pass_on_disk(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a")
    stage = evidence / "route_live_certification.json"
    stage.write_text(
        json.dumps({"passed": True, "evidence_status": "PASS", "blockers": ["forged"]}),
        encoding="utf-8",
    )

    report = _service().publish(_request(tmp_path, evidence))

    assert report.has_input_failures is True
    assert report.rows[0].status == "experimental"
    assert "route_matrix.stage_digest_mismatch:route_live_evidence_bundle" in report.rows[0].blockers


def test_symlinked_evidence_file_is_rejected(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a")
    release_path = evidence / "release-set.json"
    outside = tmp_path / "outside.json"
    release_path.replace(outside)
    release_path.symlink_to(outside)

    report = _service().publish(_request(tmp_path, evidence))

    assert report.has_input_failures is True
    assert "route_matrix.evidence_file_unsafe" in report.rows[0].blockers


def test_exact_republication_is_noop_but_conflicting_output_fails(tmp_path: Path) -> None:
    request = _request(tmp_path)
    service = _service()
    first = service.publish(request)
    second = service.publish(request)
    assert first.to_dict() == second.to_dict()
    (tmp_path / "out/route-certification-matrix.md").write_text("changed\n", encoding="utf-8")

    with pytest.raises(RouteCertificationMatrixError) as exc:
        service.publish(request)

    assert exc.value.code == "DPONE_ROUTE_MATRIX_OUTPUT_CONFLICT"


def test_republication_with_later_clock_preserves_original_bytes(tmp_path: Path) -> None:
    request = _request(tmp_path)
    first = RouteCertificationMatrixService(clock=lambda: _NOW).publish(request)
    original = first.json_path.read_bytes()

    second = RouteCertificationMatrixService(clock=lambda: _NOW + timedelta(minutes=5)).publish(request)

    assert second.evaluated_at == first.evaluated_at
    assert second.json_path.read_bytes() == original


def test_output_parent_symlink_is_rejected(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(outside, target_is_directory=True)
    request = RouteCertificationMatrixRequest(
        expected_commit=_COMMIT,
        evidence_dirs=tuple(),
        output_dir=linked / "out",
    )

    with pytest.raises(RouteCertificationMatrixError) as exc:
        _service().publish(request)

    assert exc.value.code == "DPONE_ROUTE_MATRIX_OUTPUT_UNSAFE"


def test_tampered_release_identity_fails_closed(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a")
    release_path = evidence / "release-set.json"
    payload = json.loads(release_path.read_text())
    payload["artifacts"]["canonical_schemas"].append(
        {"id": "changed", "path": "schemas/changed.json", "sha256": "sha256:" + "c" * 64}
    )
    _write_json(release_path, payload)

    report = _service().publish(_request(tmp_path, evidence))

    assert report.has_input_failures is True
    assert "route_matrix.release_digest_mismatch" in report.rows[0].blockers


def test_partial_production_proof_fails_instead_of_falling_back(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a", deployment="a")
    (evidence / "route-attestation-verification.json").unlink()

    report = _service().publish(_request(tmp_path, evidence))

    assert report.rows[0].status == "experimental"
    assert report.rows[0].production_status == "FAIL"
    assert "route_matrix.production_proof_incomplete" in report.rows[0].blockers


def test_unknown_route_evidence_is_reported_without_hiding_catalog(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a")
    bundle_path = evidence / "route_certification_bundle.json"
    bundle = json.loads(bundle_path.read_text())
    bundle["route"]["source"] = "unknown"
    _write_json(bundle_path, bundle)

    report = _service().publish(_request(tmp_path, evidence))

    assert report.has_input_failures is True
    assert report.rows[0].status == "experimental"
    assert report.errors[0].code == "route_matrix.route_unknown_or_ambiguous"


def test_certify_routes_cli_writes_matrix_and_returns_nonzero_when_unverified(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _stub_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "certify",
                "routes",
                "--commit-sha",
                _COMMIT,
                "--output-dir",
                str(tmp_path / "out"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    assert json.loads(capsys.readouterr().out)["schema"] == "dpone.route-certification-matrix.v1"
    assert (tmp_path / "out/route-certification-matrix.md").is_file()


def test_certify_routes_cli_allow_unverified_returns_zero_for_publish_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _stub_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "certify",
                "routes",
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


def test_certify_routes_cli_uses_structured_usage_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _stub_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "certify",
                "routes",
                "--commit-sha",
                "bad commit",
                "--output-dir",
                str(tmp_path / "out"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2
    error = json.loads(capsys.readouterr().out)
    assert error["schema"] == "dpone.error.v1"
    assert error["code"] == "DPONE_ROUTE_MATRIX_COMMIT_INVALID"


def test_forged_attestation_certification_claim_cannot_promote(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a", deployment="a")
    attestation_path = evidence / "route-attestation.json"
    attestation = json.loads(attestation_path.read_text())
    attestation["claims"]["certification"]["profile"] = "oss_safe"
    attestation["attestation_id"] = route_attestation_id(attestation["claims"])
    attestation_bytes = _write_json(attestation_path, attestation)
    verification_path = evidence / "route-attestation-verification.json"
    verification = json.loads(verification_path.read_text())
    verification["attestation_id"] = attestation["attestation_id"]
    verification["attestation_sha256"] = sha256_bytes(attestation_bytes)
    _write_json(verification_path, verification)

    report = _service().publish(_request(tmp_path, evidence))

    assert report.rows[0].status == "experimental"
    assert "route_matrix.attestation_certification_invalid" in report.rows[0].blockers


def test_bundle_claim_must_bind_release_commit_and_all_route_dimensions(tmp_path: Path) -> None:
    evidence = _write_evidence_set(tmp_path / "evidence-a")
    bundle_path = evidence / "route_certification_bundle.json"
    bundle = json.loads(bundle_path.read_text())
    bundle["matrix_claim"]["route"]["transport"] = "different"
    _write_json(bundle_path, bundle)

    report = _service().publish(_request(tmp_path, evidence))

    assert report.has_input_failures is True
    assert report.rows[0].status == "experimental"
    assert "route_matrix.bundle_claim_invalid" in report.rows[0].blockers


def _accepting_crypto_gate(
    *,
    directory: Path,
    attestation_path: Path,
    certification_bundle_path: Path,
    expected: RouteAttestationExpectedSubject,
) -> str | None:
    del directory, attestation_path, certification_bundle_path, expected
    return None


def _service() -> RouteCertificationMatrixService:
    return RouteCertificationMatrixService(
        reader=RouteCertificationEvidenceReader(crypto_gate=_accepting_crypto_gate),
        clock=lambda: _NOW,
    )


def _stub_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = SimpleNamespace(info=lambda *args: None, error=lambda *args: None)
    monkeypatch.setattr(cli_main, "setup_logging", lambda: logger)
    monkeypatch.setattr(
        cli_main.AppContext,
        "from_env",
        staticmethod(lambda *, logger: SimpleNamespace(logger=logger)),
    )


def _request(root: Path, *evidence: Path) -> RouteCertificationMatrixRequest:
    return RouteCertificationMatrixRequest(
        expected_commit=_COMMIT,
        evidence_dirs=tuple(evidence),
        output_dir=root / "out",
        max_age_hours=168,
    )


def _write_evidence_set(
    root: Path,
    *,
    source_commit: str = _COMMIT,
    deployment: str | None = None,
    signer: str = "ci://dpone",
) -> Path:
    root.mkdir(parents=True)
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {"dag_specs": [], "workload_packs": [], "canonical_schemas": []},
        "provenance": {"source_commit": source_commit, "build_id": "pytest", "built_at": "2026-07-16T10:00:00Z"},
    }
    release["release_id"] = release_id(release)
    _write_json(root / "release-set.json", release)
    live_stage = {
        "schema_version": "dpone.route_live_certification.v1",
        "evidence_status": "PASS",
        "passed": True,
        "blockers": [],
        "route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"},
    }
    live_stage_bytes = _write_json(root / "route_live_certification.json", live_stage)
    bundle = _bundle(release_id=str(release["release_id"]), source_commit=source_commit)
    stages = bundle["stages"]
    assert isinstance(stages, list)
    stages[0]["sha256"] = sha256_bytes(live_stage_bytes).removeprefix("sha256:")
    bundle_path = root / "route_certification_bundle.json"
    bundle_bytes = _write_json(bundle_path, bundle)
    if deployment is not None:
        claims = {
            "route": dict(_ROUTE),
            "certification": {
                "bundle_sha256": sha256_bytes(bundle_bytes),
                "profile": "vendor_live",
                "level": "certified",
            },
            "subject": {
                "release_id": release["release_id"],
                "deployment_id": "sha256:" + deployment * 64,
                "environment": "production",
                "runtime_image_digest": "sha256:" + "f" * 64,
                "authorization_profile": "safe_sample_production",
            },
            "validity": {
                "issued_at": "2026-07-16T10:00:00Z",
                "not_before": "2026-07-16T10:00:00Z",
                "expires_at": "2026-07-17T10:00:00Z",
            },
        }
        attestation = {
            "schema": "dpone.route-attestation.v1",
            "attestation_id": route_attestation_id(claims),
            "claims": claims,
        }
        attestation_bytes = _write_json(root / "route-attestation.json", attestation)
        verification = {
            "schema": "dpone.route-attestation-verification.v1",
            "decision": "verified",
            "code": "DPONE_ROUTE_ATTESTATION_VERIFIED",
            "message": "verified",
            "attestation_id": attestation["attestation_id"],
            "attestation_sha256": sha256_bytes(attestation_bytes),
            "certification_bundle_sha256": sha256_bytes(bundle_bytes),
            "policy_fingerprint": "sha256:" + "e" * 64,
            "route_id": _ROUTE_ID,
            "release_id": release["release_id"],
            "deployment_id": claims["subject"]["deployment_id"],
            "environment": "production",
            "authorization_profile": "safe_sample_production",
            "signer": {
                "backend": "cosign_keyless_v1",
                "certificate_identity": signer,
                "certificate_oidc_issuer": "https://token.actions.githubusercontent.com",
                "verifier_version": "3.0.4",
            },
            "validity": {
                "not_before": "2026-07-16T10:00:00Z",
                "expires_at": "2026-07-17T10:00:00Z",
            },
            "verified_at": "2026-07-16T11:00:00Z",
            "errors": [],
        }
        _write_json(root / "route-attestation-verification.json", verification)
    return root


def _bundle(*, release_id: str, source_commit: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.route_certification_bundle.v1",
        "evidence_status": "PASS",
        "release": "v0.72.3",
        "profile": "vendor_live",
        "route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"},
        "route_profile": None,
        "passed": True,
        "level": "certified",
        "score": 100.0,
        "blockers": [],
        "warnings": [],
        "next_actions": [],
        "required_evidence": ["route_live_evidence_bundle"],
        "matrix_claim": {
            "schema": "dpone.route-matrix-claim.v1",
            "release_id": release_id,
            "source_commit": source_commit,
            "certified_at": _NOW.isoformat().replace("+00:00", "Z"),
            "route": dict(_ROUTE),
        },
        "stages": [
            {
                "name": "route_live_evidence_bundle",
                "path": "route_live_certification.json",
                "sha256": "",
                "passed": True,
                "required": True,
                "summary": "approved live route evidence",
                "blockers": [],
            }
        ],
        "artifact_index": {},
        "output_dir": ".",
        "json_path": "route_certification_bundle.json",
        "markdown_path": "route_certification_bundle.md",
    }


def _write_json(path: Path, payload: object) -> bytes:
    content = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    path.write_bytes(content)
    return content
