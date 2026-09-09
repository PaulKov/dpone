from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.cdc.recovery import CdcRecoveryEvidenceService
from dpone.ops.cdc.recovery_models import CdcFailureScenario, CdcRecoveryPolicy


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


def _handoff_payload(*, passed: bool = True) -> dict[str, object]:
    return {
        "schema_version": "dpone.cdc_handoff.v1",
        "stream": _stream_payload(),
        "route": _route_payload(),
        "passed": passed,
        "blockers": [] if passed else ["cdc_window.invalid"],
    }


def _apply_payload(*, passed: bool = True) -> dict[str, object]:
    return {
        "schema_version": "dpone.cdc_apply_certification.v1",
        "stream": _stream_payload(),
        "route": _route_payload(),
        "passed": passed,
        "evidence_status": "PASS" if passed else "FAIL",
        "blockers": [] if passed else ["typed_cdc_hash.mismatch"],
        "metrics": {"events_seen": 1000, "events_applied": 1000, "duplicate_events": 1},
    }


def _observability_payload(*, passed: bool = True) -> dict[str, object]:
    return {
        "schema_version": "dpone.cdc_observability.v1",
        "stream": _stream_payload(),
        "route": _route_payload(),
        "passed": passed,
        "blockers": [] if passed else ["cdc_lag_slo.exceeded"],
        "metrics": {
            "lag_seconds": 20,
            "freshness_seconds": 30,
            "retention_remaining_seconds": 7200,
            "events_per_second": 900,
            "offset_committed": True,
        },
    }


def _scenario_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "scenario_id": "orders-consumer-restart",
        "kind": "consumer_restart",
        "injected_at": "2026-06-12T12:05:00Z",
        "failed_after_offset": "0x12",
        "recovered_offset": "0x14",
        "resume_completed": True,
        "sink_commit_state": "committed",
        "offset_commit_state": "committed",
        "offset_committed_after_sink_commit": True,
        "partial_sink_commit_detected": True,
        "partial_sink_commit_repaired": True,
        "repair_actions": ["truncate staging", "replay bounded CDC window"],
        "replayed_events": 10,
        "duplicate_events": 2,
        "idempotent_replay_passed": True,
        "poison_events": 1,
        "quarantined_events": 1,
        "retention_remaining_seconds": 3600,
        "recovery_margin_seconds": 2400,
    }
    payload.update(overrides)
    return payload


def test_cdc_failure_scenario_and_policy_normalize_json() -> None:
    scenario = CdcFailureScenario.from_dict(_scenario_payload(kind="duplicate_replay"))
    policy = CdcRecoveryPolicy.from_dict({"min_recovery_margin_seconds": 1200, "max_duplicate_events": 3})

    assert scenario.kind == "duplicate_replay"
    assert scenario.resume_completed is True
    assert scenario.repair_actions == ("truncate staging", "replay bounded CDC window")
    assert policy.min_recovery_margin_seconds == 1200
    assert policy.max_duplicate_events == 3


def test_cdc_recovery_evidence_passes_and_writes_all_domains(tmp_path: Path) -> None:
    handoff = _write_json(tmp_path / "handoff.json", _handoff_payload())
    apply = _write_json(tmp_path / "apply.json", _apply_payload())
    observability = _write_json(tmp_path / "observability.json", _observability_payload())
    scenario = _write_json(tmp_path / "scenario.json", _scenario_payload())
    policy = _write_json(tmp_path / "policy.json", {"max_duplicate_events": 5, "max_replayed_events": 20})

    report = CdcRecoveryEvidenceService().evaluate(
        output_dir=tmp_path / "recovery",
        handoff_json=handoff,
        apply_certification_json=apply,
        observability_json=observability,
        scenario_json=scenario,
        policy_json=policy,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    expected_domains = {
        "cdc_restart_resume",
        "cdc_offset_commit_ordering",
        "cdc_idempotent_replay_window",
        "cdc_partial_commit_repair",
        "cdc_poison_event_quarantine",
        "cdc_retention_recovery_margin",
    }

    assert report.passed is True
    assert payload["schema_version"] == "dpone.cdc_recovery_evidence.v1"
    assert payload["stream"]["stream_id"] == "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert payload["scenario"]["kind"] == "consumer_restart"
    assert set(payload["evidence_artifacts"]) == expected_domains
    assert Path(payload["evidence_artifacts"]["cdc_restart_resume"]).exists()
    assert "CDC recovery evidence" in Path(report.markdown_path).read_text(encoding="utf-8")


def test_cdc_recovery_evidence_blocks_unsafe_fault_recovery(tmp_path: Path) -> None:
    handoff = _write_json(tmp_path / "handoff.json", _handoff_payload())
    apply = _write_json(tmp_path / "apply.json", _apply_payload())
    observability = _write_json(tmp_path / "observability.json", _observability_payload())
    scenario = _write_json(
        tmp_path / "scenario.json",
        _scenario_payload(
            kind="partial_sink_commit",
            resume_completed=False,
            recovered_offset="",
            offset_commit_state="committed",
            sink_commit_state="partial",
            offset_committed_after_sink_commit=False,
            partial_sink_commit_detected=True,
            partial_sink_commit_repaired=False,
            idempotent_replay_passed=False,
            duplicate_events=9,
            replayed_events=50,
            poison_events=2,
            quarantined_events=1,
            retention_remaining_seconds=60,
            recovery_margin_seconds=60,
        ),
    )

    report = CdcRecoveryEvidenceService().evaluate(
        output_dir=tmp_path / "recovery",
        handoff_json=handoff,
        apply_certification_json=apply,
        observability_json=observability,
        scenario_json=scenario,
    )

    assert report.passed is False
    assert "cdc_restart_resume.not_resumed" in report.blockers
    assert "cdc_offset_commit_ordering.unsafe" in report.blockers
    assert "cdc_idempotent_replay_window.failed" in report.blockers
    assert "cdc_partial_commit_repair.unrepaired" in report.blockers
    assert "cdc_poison_event_quarantine.missing" in report.blockers
    assert "cdc_retention_recovery_margin.too_low" in report.blockers


def test_cdc_recovery_evidence_includes_upstream_blockers(tmp_path: Path) -> None:
    handoff = _write_json(tmp_path / "handoff.json", _handoff_payload(passed=False))
    apply = _write_json(tmp_path / "apply.json", _apply_payload(passed=False))
    observability = _write_json(tmp_path / "observability.json", _observability_payload(passed=False))
    scenario = _write_json(tmp_path / "scenario.json", _scenario_payload())

    report = CdcRecoveryEvidenceService().evaluate(
        output_dir=tmp_path / "recovery",
        handoff_json=handoff,
        apply_certification_json=apply,
        observability_json=observability,
        scenario_json=scenario,
    )

    assert report.passed is False
    assert "cdc_handoff.not_passed" in report.blockers
    assert "cdc_apply_certification.not_passed" in report.blockers
    assert "cdc_observability.not_passed" in report.blockers
