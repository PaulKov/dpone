from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from jsonschema import Draft7Validator

from dpone._compat import UTC
from dpone.contracts.catalog_bundle import sha256_bytes
from dpone.contracts.extension_conformance import ExtensionConformanceError
from dpone.services.extension_conformance import ExtensionConformanceService

_SUBJECT = "sha256:" + "a" * 64


def _write_receipt(root: Path, check: str, status: str = "PASS") -> dict[str, str]:
    path = root / f"evidence/{check}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "dpone.extension-check-receipt.v1",
        "check": check,
        "subject_digest": _SUBJECT,
        "status": status,
        "producer": "pytest",
        "command": f"pytest -k {check}",
    }
    content = (json.dumps(payload, sort_keys=True) + "\n").encode()
    path.write_bytes(content)
    return {
        "check": check,
        "artifact_ref": path.relative_to(root).as_posix(),
        "sha256": sha256_bytes(content),
        "expected_schema": "dpone.extension-check-receipt.v1",
    }


def _request(root: Path, *, statuses: dict[str, str] | None = None) -> dict[str, object]:
    statuses = statuses or {}
    checks = ["public_contract", "provider_discovery", "airflow_matrix", "package_smoke", "parse_budget"]
    return {
        "schema": "dpone.extension-conformance-request.v1",
        "profile": "airflow_provider",
        "subject": {"id": "apache-airflow-providers-dpone", "version": "0.72.6", "digest": _SUBJECT},
        "evidence": [_write_receipt(root, check, statuses.get(check, "PASS")) for check in checks],
    }


def test_all_required_exact_evidence_produces_pass(tmp_path: Path) -> None:
    service = ExtensionConformanceService(clock=lambda: datetime(2026, 7, 16, 12, tzinfo=UTC))
    report = service.evaluate(_request(tmp_path), project_root=tmp_path)

    assert report.status == "PASS"
    assert all(item.status == "PASS" for item in report.checks)
    service.write(report, output_dir=Path("test_artifacts/provider"), project_root=tmp_path)
    payload = json.loads((tmp_path / "test_artifacts/provider/extension-conformance.json").read_text())
    assert payload["status"] == "PASS"
    schema = json.loads(
        (Path(__file__).parents[1] / "src/dpone/schema/extension-conformance.schema.json").read_text(encoding="utf-8")
    )
    Draft7Validator.check_schema(schema)
    Draft7Validator(schema).validate(payload)


def test_missing_required_evidence_is_unverified_never_pass(tmp_path: Path) -> None:
    request = _request(tmp_path)
    request["evidence"] = request["evidence"][:-1]

    report = ExtensionConformanceService().evaluate(request, project_root=tmp_path)

    assert report.status == "UNVERIFIED"
    assert report.checks[-1].code == "DPONE_EXTENSION_EVIDENCE_MISSING"


def test_fail_dominates_unverified(tmp_path: Path) -> None:
    request = _request(tmp_path, statuses={"public_contract": "FAIL"})
    request["evidence"] = request["evidence"][:-1]

    report = ExtensionConformanceService().evaluate(request, project_root=tmp_path)

    assert report.status == "FAIL"


def test_digest_mismatch_fails_closed(tmp_path: Path) -> None:
    request = _request(tmp_path)
    request["evidence"][0]["sha256"] = "sha256:" + "b" * 64

    report = ExtensionConformanceService().evaluate(request, project_root=tmp_path)

    assert report.status == "FAIL"
    assert report.checks[0].code == "DPONE_EXTENSION_EVIDENCE_DIGEST_MISMATCH"


def test_unknown_or_duplicate_check_is_rejected(tmp_path: Path) -> None:
    request = _request(tmp_path)
    request["evidence"][0]["check"] = "arbitrary_plugin_code"

    with pytest.raises(ExtensionConformanceError) as exc:
        ExtensionConformanceService().evaluate(request, project_root=tmp_path)

    assert exc.value.code == "DPONE_EXTENSION_CONFORMANCE_REQUEST_INVALID"


def test_route_profile_cannot_pass_without_live_evidence(tmp_path: Path) -> None:
    checks = ["route_contract", "reconciliation", "recovery", "state_evidence_ordering"]
    request = {
        "schema": "dpone.extension-conformance-request.v1",
        "profile": "route",
        "subject": {"id": "mssql-clickhouse", "version": "1", "digest": _SUBJECT},
        "evidence": [_write_receipt(tmp_path, check) for check in checks],
    }

    report = ExtensionConformanceService().evaluate(request, project_root=tmp_path)

    assert report.status == "UNVERIFIED"
    assert report.checks[-1].check == "live"
