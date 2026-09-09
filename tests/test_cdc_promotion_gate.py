from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.cdc.promotion import CdcPromotionGateService
from dpone.ops.cdc.promotion_models import CdcPromotionDecision, CdcPromotionEvidenceItem


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _stream_payload(*, target_dataset: str = "analytics.orders") -> dict[str, object]:
    stream_id_target = target_dataset.replace(".", "_")
    return {
        "source": "mssql",
        "sink": "clickhouse",
        "strategy": "cdc",
        "source_dataset": "dbo.orders",
        "target_dataset": target_dataset,
        "stream_id": f"mssql_to_clickhouse__cdc__dbo_orders__{stream_id_target}",
        "colon_id": f"mssql:clickhouse:cdc:dbo.orders:{target_dataset}",
    }


def _route_payload() -> dict[str, object]:
    return {
        "source": "mssql",
        "sink": "clickhouse",
        "strategy": "cdc",
        "case_id": "mssql_to_clickhouse__cdc",
        "colon_id": "mssql:clickhouse:cdc",
    }


def _upstream_payload(
    schema_version: str, *, passed: bool = True, target_dataset: str = "analytics.orders"
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": schema_version,
        "stream": _stream_payload(target_dataset=target_dataset),
        "route": _route_payload(),
        "passed": passed,
        "blockers": [] if passed else ["upstream.failed"],
        "warnings": [],
    }
    if schema_version == "dpone.cdc_apply_certification.v1":
        payload["evidence_status"] = "PASS" if passed else "FAIL"
    return payload


def test_cdc_promotion_evidence_item_and_decision_are_serializable() -> None:
    item = CdcPromotionEvidenceItem(
        name="cdc_apply_gate",
        passed=True,
        summary="CDC apply evidence is green",
        blockers=tuple(),
        details={"schema_version": "dpone.cdc_apply_certification.v1"},
    )
    decision = CdcPromotionDecision(
        passed=True,
        production_ready=True,
        promote_offsets=True,
        blockers=tuple(),
        warnings=("review runtime sink commit",),
        next_actions=("Promote offsets after sink commit.",),
    )

    assert item.to_dict()["name"] == "cdc_apply_gate"
    assert decision.to_dict()["production_ready"] is True
    assert decision.to_dict()["promote_offsets"] is True


def test_cdc_promotion_gate_passes_and_writes_all_domains(tmp_path: Path) -> None:
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1"))
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    recovery = _write_json(tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1"))
    schema = _write_json(tmp_path / "schema.json", _upstream_payload("dpone.cdc_schema_evolution_evidence.v1"))

    report = CdcPromotionGateService().evaluate(
        output_dir=tmp_path / "promotion",
        apply_certification_json=apply,
        handoff_json=handoff,
        observability_json=observability,
        recovery_json=recovery,
        schema_evolution_json=schema,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    expected_domains = {
        "cdc_apply_gate",
        "cdc_handoff_gate",
        "cdc_observability_gate",
        "cdc_recovery_gate",
        "cdc_schema_evolution_gate",
        "cdc_stream_identity_consistency",
        "cdc_offset_promotion_decision",
    }

    assert report.passed is True
    assert report.production_ready is True
    assert report.promote_offsets is True
    assert payload["schema_version"] == "dpone.cdc_promotion_gate.v1"
    assert payload["stream"]["stream_id"] == "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert payload["production_ready"] is True
    assert payload["promote_offsets"] is True
    assert set(payload["evidence_artifacts"]) == expected_domains
    assert Path(payload["evidence_artifacts"]["cdc_offset_promotion_decision"]).exists()
    assert "CDC promotion gate" in Path(report.markdown_path).read_text(encoding="utf-8")


def test_cdc_promotion_gate_blocks_failed_upstream_and_offset_promotion(tmp_path: Path) -> None:
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1", passed=False))
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    recovery = _write_json(tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1"))
    schema = _write_json(tmp_path / "schema.json", _upstream_payload("dpone.cdc_schema_evolution_evidence.v1"))

    report = CdcPromotionGateService().evaluate(
        output_dir=tmp_path / "promotion",
        apply_certification_json=apply,
        handoff_json=handoff,
        observability_json=observability,
        recovery_json=recovery,
        schema_evolution_json=schema,
    )

    assert report.passed is False
    assert report.production_ready is False
    assert report.promote_offsets is False
    assert "cdc_apply_gate.not_passed" in report.blockers
    assert "cdc_offset_promotion_decision.blocked" in report.blockers


def test_cdc_promotion_gate_rejects_statusless_apply_certification(tmp_path: Path) -> None:
    apply_payload = _upstream_payload("dpone.cdc_apply_certification.v1")
    apply_payload.pop("evidence_status")
    report = CdcPromotionGateService().evaluate(
        output_dir=tmp_path / "promotion",
        apply_certification_json=_write_json(tmp_path / "apply.json", apply_payload),
        handoff_json=_write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1")),
        observability_json=_write_json(
            tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1")
        ),
        recovery_json=_write_json(tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1")),
        schema_evolution_json=_write_json(
            tmp_path / "schema.json", _upstream_payload("dpone.cdc_schema_evolution_evidence.v1")
        ),
    )

    assert report.passed is False
    assert "cdc_apply_gate.not_passed" in report.blockers


def test_cdc_promotion_gate_blocks_stream_identity_mismatch(tmp_path: Path) -> None:
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1"))
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    recovery = _write_json(
        tmp_path / "recovery.json",
        _upstream_payload("dpone.cdc_recovery_evidence.v1", target_dataset="analytics.orders_shadow"),
    )
    schema = _write_json(tmp_path / "schema.json", _upstream_payload("dpone.cdc_schema_evolution_evidence.v1"))

    report = CdcPromotionGateService().evaluate(
        output_dir=tmp_path / "promotion",
        apply_certification_json=apply,
        handoff_json=handoff,
        observability_json=observability,
        recovery_json=recovery,
        schema_evolution_json=schema,
    )

    assert report.passed is False
    assert "cdc_stream_identity_consistency.mismatch" in report.blockers
    assert "cdc_offset_promotion_decision.blocked" in report.blockers


def test_cdc_promotion_gate_blocks_missing_stream_identity(tmp_path: Path) -> None:
    payload = _upstream_payload("dpone.cdc_apply_certification.v1")
    payload.pop("stream")
    apply = _write_json(tmp_path / "apply.json", payload)
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    recovery = _write_json(tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1"))
    schema = _write_json(tmp_path / "schema.json", _upstream_payload("dpone.cdc_schema_evolution_evidence.v1"))

    report = CdcPromotionGateService().evaluate(
        output_dir=tmp_path / "promotion",
        apply_certification_json=apply,
        handoff_json=handoff,
        observability_json=observability,
        recovery_json=recovery,
        schema_evolution_json=schema,
    )

    assert report.passed is False
    assert report.production_ready is False
    assert "cdc_stream_identity_consistency.mismatch" in report.blockers
