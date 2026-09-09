from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.cdc.observability import CdcObservabilityEvidenceService
from dpone.ops.cdc.observability_models import CdcSloProfile, CdcTelemetrySnapshot


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _handoff_payload(*, passed: bool = True) -> dict[str, object]:
    return {
        "schema_version": "dpone.cdc_handoff.v1",
        "stream": {
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "cdc",
            "source_dataset": "dbo.orders",
            "target_dataset": "analytics.orders",
            "stream_id": "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders",
            "colon_id": "mssql:clickhouse:cdc:dbo.orders:analytics.orders",
        },
        "route": {
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "cdc",
            "case_id": "mssql_to_clickhouse__cdc",
            "colon_id": "mssql:clickhouse:cdc",
        },
        "passed": passed,
        "blockers": [] if passed else ["cdc_window.invalid"],
    }


def _apply_payload(*, passed: bool = True) -> dict[str, object]:
    return {
        "schema_version": "dpone.cdc_apply_certification.v1",
        "stream": {
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "cdc",
            "source_dataset": "dbo.orders",
            "target_dataset": "analytics.orders",
            "stream_id": "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders",
            "colon_id": "mssql:clickhouse:cdc:dbo.orders:analytics.orders",
        },
        "route": {
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "cdc",
            "case_id": "mssql_to_clickhouse__cdc",
            "colon_id": "mssql:clickhouse:cdc",
        },
        "passed": passed,
        "evidence_status": "PASS" if passed else "FAIL",
        "blockers": [] if passed else ["typed_cdc_hash.mismatch"],
        "metrics": {
            "events_seen": 1000,
            "events_applied": 1000,
            "duplicate_events": 1,
        },
    }


def _metrics_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "captured_at": "2026-06-12T12:00:00Z",
        "lag_seconds": 30,
        "freshness_seconds": 45,
        "retention_remaining_seconds": 7200,
        "events_per_second": 1200,
        "duplicate_events": 1,
        "replayed_events": 2,
        "offset_commit_status": "committed",
        "last_committed_offset": "0x13",
    }
    payload.update(overrides)
    return payload


def test_cdc_telemetry_snapshot_and_slo_profile_normalize_json() -> None:
    snapshot = CdcTelemetrySnapshot.from_dict(_metrics_payload())
    slo = CdcSloProfile.from_dict({"max_lag_seconds": 60, "max_duplicate_events": 2})

    assert snapshot.lag_seconds == 30
    assert snapshot.offset_committed is True
    assert slo.max_lag_seconds == 60
    assert slo.max_duplicate_events == 2


def test_cdc_observability_evidence_passes_and_writes_all_domains(tmp_path: Path) -> None:
    handoff = _write_json(tmp_path / "handoff.json", _handoff_payload())
    apply = _write_json(tmp_path / "apply.json", _apply_payload())
    metrics = _write_json(tmp_path / "metrics.json", _metrics_payload())
    slo = _write_json(tmp_path / "slo.json", {"max_duplicate_events": 2, "min_events_per_second": 100})

    report = CdcObservabilityEvidenceService().evaluate(
        output_dir=tmp_path / "obs",
        handoff_json=handoff,
        apply_certification_json=apply,
        metrics_json=metrics,
        slo_json=slo,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    expected_domains = {
        "cdc_lag_slo",
        "cdc_freshness_slo",
        "cdc_retention_risk",
        "cdc_offset_commit_health",
        "cdc_duplicate_replay_rate",
        "cdc_throughput_slo",
    }

    assert report.passed is True
    assert payload["schema_version"] == "dpone.cdc_observability.v1"
    assert payload["stream"]["stream_id"] == "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert set(payload["evidence_artifacts"]) == expected_domains
    assert payload["metrics"]["lag_seconds"] == 30
    assert Path(payload["evidence_artifacts"]["cdc_lag_slo"]).exists()
    assert "CDC observability evidence" in Path(report.markdown_path).read_text(encoding="utf-8")


def test_cdc_observability_evidence_blocks_lag_retention_and_offset_failures(tmp_path: Path) -> None:
    handoff = _write_json(tmp_path / "handoff.json", _handoff_payload())
    apply = _write_json(tmp_path / "apply.json", _apply_payload())
    metrics = _write_json(
        tmp_path / "metrics.json",
        _metrics_payload(
            lag_seconds=900,
            retention_remaining_seconds=60,
            offset_commit_status="stale",
            last_committed_offset="",
        ),
    )

    report = CdcObservabilityEvidenceService().evaluate(
        output_dir=tmp_path / "obs",
        handoff_json=handoff,
        apply_certification_json=apply,
        metrics_json=metrics,
    )

    assert report.passed is False
    assert "cdc_lag_slo.exceeded" in report.blockers
    assert "cdc_retention_risk.window_not_available" in report.blockers
    assert "cdc_offset_commit_health.not_committed" in report.blockers


def test_cdc_observability_evidence_includes_upstream_certification_blockers(tmp_path: Path) -> None:
    handoff = _write_json(tmp_path / "handoff.json", _handoff_payload(passed=False))
    apply = _write_json(tmp_path / "apply.json", _apply_payload(passed=False))
    metrics = _write_json(tmp_path / "metrics.json", _metrics_payload())

    report = CdcObservabilityEvidenceService().evaluate(
        output_dir=tmp_path / "obs",
        handoff_json=handoff,
        apply_certification_json=apply,
        metrics_json=metrics,
    )

    assert report.passed is False
    assert "cdc_handoff.not_passed" in report.blockers
    assert "cdc_apply_certification.not_passed" in report.blockers
