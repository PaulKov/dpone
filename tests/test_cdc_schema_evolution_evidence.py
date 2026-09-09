from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.cdc.schema_evolution import CdcSchemaEvolutionEvidenceService
from dpone.ops.cdc.schema_evolution_models import (
    CdcSchemaChangeEvent,
    CdcSchemaEvolutionPlan,
    CdcSchemaEvolutionPolicy,
)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _stream_payload() -> dict[str, object]:
    return {
        "source": "mssql",
        "sink": "clickhouse",
        "strategy": "cdc",
        "source_dataset": "dbo.orders",
        "target_dataset": "analytics.orders",
        "stream_id": "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders",
        "colon_id": "mssql:clickhouse:cdc:dbo.orders:analytics.orders",
    }


def _route_payload() -> dict[str, object]:
    return {
        "source": "mssql",
        "sink": "clickhouse",
        "strategy": "cdc",
        "case_id": "mssql_to_clickhouse__cdc",
        "colon_id": "mssql:clickhouse:cdc",
    }


def _upstream_payload(schema_version: str, *, passed: bool = True) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": schema_version,
        "stream": _stream_payload(),
        "route": _route_payload(),
        "passed": passed,
        "blockers": [] if passed else ["upstream.failed"],
    }
    if schema_version == "dpone.cdc_apply_certification.v1":
        payload["evidence_status"] = "PASS" if passed else "FAIL"
    return payload


def _schema_change_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "change": {
            "change_id": "orders-add-status-reason",
            "kind": "add_column",
            "captured_at": "2026-06-12T12:20:00Z",
            "source_table": "dbo.orders",
            "source_column": "status_reason",
            "target_table": "analytics.orders",
            "target_column": "status_reason",
            "old_type": "",
            "new_type": "nvarchar(100)",
            "old_nullable": True,
            "new_nullable": True,
            "source_offset": "0x15",
            "breaking": False,
        },
        "plan": {
            "sink_impact": "add nullable String column",
            "compatibility_level": "compatible",
            "target_ddl_preview": "ALTER TABLE analytics.orders ADD COLUMN status_reason Nullable(String)",
            "ddl_dry_run_passed": True,
            "backfill_required": False,
            "backfill_plan": "",
            "type_widening_safe": True,
            "offset_schema_ordering_safe": True,
            "approved_by": ["data-architect"],
        },
    }
    for key, value in overrides.items():
        section, _, field = key.partition("__")
        if section in {"change", "plan"} and field:
            nested = payload[section]
            assert isinstance(nested, dict)
            nested[field] = value
        else:
            payload[key] = value
    return payload


def test_cdc_schema_change_and_plan_normalize_json() -> None:
    payload = _schema_change_payload(change__kind="alter_type", plan__compatibility_level="backward_compatible")
    change = CdcSchemaChangeEvent.from_dict(payload["change"])  # type: ignore[arg-type]
    plan = CdcSchemaEvolutionPlan.from_dict(payload["plan"])  # type: ignore[arg-type]
    policy = CdcSchemaEvolutionPolicy.from_dict({"allowed_compatibility_levels": ["compatible"]})

    assert change.kind == "alter_type"
    assert change.change_id == "orders-add-status-reason"
    assert plan.compatibility_level == "backward_compatible"
    assert plan.approved_by == ("data-architect",)
    assert policy.allowed_compatibility_levels == ("compatible",)


def test_cdc_schema_evolution_evidence_passes_and_writes_all_domains(tmp_path: Path) -> None:
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    recovery = _write_json(tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1"))
    schema_change = _write_json(tmp_path / "schema_change.json", _schema_change_payload())

    report = CdcSchemaEvolutionEvidenceService().evaluate(
        output_dir=tmp_path / "schema",
        handoff_json=handoff,
        apply_certification_json=apply,
        observability_json=observability,
        recovery_json=recovery,
        schema_change_json=schema_change,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    expected_domains = {
        "cdc_schema_change_capture",
        "cdc_schema_compatibility",
        "cdc_type_widening_safety",
        "cdc_target_ddl_dry_run",
        "cdc_backfill_requirement",
        "cdc_breaking_change_gate",
        "cdc_offset_schema_ordering",
    }

    assert report.passed is True
    assert payload["schema_version"] == "dpone.cdc_schema_evolution_evidence.v1"
    assert payload["stream"]["stream_id"] == "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert payload["change"]["kind"] == "add_column"
    assert payload["plan"]["compatibility_level"] == "compatible"
    assert set(payload["evidence_artifacts"]) == expected_domains
    assert Path(payload["evidence_artifacts"]["cdc_target_ddl_dry_run"]).exists()
    assert "CDC schema evolution evidence" in Path(report.markdown_path).read_text(encoding="utf-8")


def test_cdc_schema_evolution_evidence_blocks_unsafe_schema_changes(tmp_path: Path) -> None:
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    recovery = _write_json(tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1"))
    schema_change = _write_json(
        tmp_path / "schema_change.json",
        _schema_change_payload(
            change__kind="alter_type",
            change__breaking=True,
            plan__compatibility_level="incompatible",
            plan__target_ddl_preview="",
            plan__ddl_dry_run_passed=False,
            plan__backfill_required=True,
            plan__backfill_plan="",
            plan__type_widening_safe=False,
            plan__offset_schema_ordering_safe=False,
            plan__approved_by=[],
        ),
    )

    report = CdcSchemaEvolutionEvidenceService().evaluate(
        output_dir=tmp_path / "schema",
        handoff_json=handoff,
        apply_certification_json=apply,
        observability_json=observability,
        recovery_json=recovery,
        schema_change_json=schema_change,
    )

    assert report.passed is False
    assert "cdc_schema_compatibility.incompatible" in report.blockers
    assert "cdc_type_widening_safety.unsafe" in report.blockers
    assert "cdc_target_ddl_dry_run.failed" in report.blockers
    assert "cdc_backfill_requirement.missing_plan" in report.blockers
    assert "cdc_breaking_change_gate.unapproved" in report.blockers
    assert "cdc_offset_schema_ordering.unsafe" in report.blockers


def test_cdc_schema_evolution_evidence_includes_upstream_blockers(tmp_path: Path) -> None:
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1", passed=False))
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1", passed=False))
    observability = _write_json(
        tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1", passed=False)
    )
    recovery = _write_json(
        tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1", passed=False)
    )
    schema_change = _write_json(tmp_path / "schema_change.json", _schema_change_payload())

    report = CdcSchemaEvolutionEvidenceService().evaluate(
        output_dir=tmp_path / "schema",
        handoff_json=handoff,
        apply_certification_json=apply,
        observability_json=observability,
        recovery_json=recovery,
        schema_change_json=schema_change,
    )

    assert report.passed is False
    assert "cdc_handoff.not_passed" in report.blockers
    assert "cdc_apply_certification.not_passed" in report.blockers
    assert "cdc_observability.not_passed" in report.blockers
    assert "cdc_recovery.not_passed" in report.blockers
