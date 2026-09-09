from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.retention import CdcRetentionGapService, CdcRetentionPolicy
from dpone.runtime.cdc.retention_models import CdcRetentionBounds
from dpone.runtime.cdc.retention_probes import StaticCdcRetentionProbe
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream


def _stream() -> CdcRuntimeStream:
    return CdcRuntimeStream(
        pipeline_name="orders-cdc",
        source="mssql",
        sink="clickhouse",
        backend=CDCBackend.MSSQL_CHANGE_TRACKING,
        source_schema="dbo",
        source_table="orders",
        target_dataset="analytics.orders_cdc",
        unique_key=("order_id",),
    )


def _bounds(*, min_available: str = "100", current: str = "150") -> CdcRetentionBounds:
    return CdcRetentionBounds(
        backend=CDCBackend.MSSQL_CHANGE_TRACKING,
        min_available_offset=min_available,
        high_watermark=current,
        current_offset=current,
        retention_seconds=172800,
    )


def test_retention_policy_detects_healthy_at_risk_and_gap(tmp_path: Path) -> None:
    service = CdcRetentionGapService(policy=CdcRetentionPolicy(at_risk_margin=5))
    stream = _stream()

    healthy = service.evaluate(
        output_dir=tmp_path / "healthy",
        stream=stream,
        committed_offset=CDCOffset(CDCBackend.MSSQL_CHANGE_TRACKING, "120", True),
        probe=StaticCdcRetentionProbe(_bounds(min_available="100", current="150")),
    )
    at_risk = service.evaluate(
        output_dir=tmp_path / "at_risk",
        stream=stream,
        committed_offset=CDCOffset(CDCBackend.MSSQL_CHANGE_TRACKING, "103", True),
        probe=StaticCdcRetentionProbe(_bounds(min_available="100", current="150")),
    )
    gap = service.evaluate(
        output_dir=tmp_path / "gap",
        stream=stream,
        committed_offset=CDCOffset(CDCBackend.MSSQL_CHANGE_TRACKING, "99", True),
        probe=StaticCdcRetentionProbe(_bounds(min_available="100", current="150")),
    )

    assert healthy.passed is True
    assert healthy.decision.level == "healthy"
    assert healthy.metrics["offset_margin"] == 20
    assert at_risk.passed is True
    assert at_risk.decision.level == "at_risk"
    assert "cdc_retention.offset_near_min_available" in at_risk.warnings
    assert gap.passed is False
    assert gap.decision.level == "gap_detected"
    assert "cdc_retention.offset_before_min_available" in gap.blockers

    payload = json.loads((tmp_path / "gap" / "cdc_retention_check.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == "dpone.cdc_retention_check.v1"
    assert payload["stream"]["route_id"] == "mssql_to_clickhouse__cdc"
    assert payload["committed_offset"]["token"] == "99"
    assert payload["bounds"]["min_available_offset"] == "100"
    assert (tmp_path / "gap" / "cdc_retention_check.md").exists()


def test_retention_policy_compares_mssql_cdc_lsn_tokens(tmp_path: Path) -> None:
    stream = CdcRuntimeStream(
        pipeline_name="orders-cdc",
        source="mssql",
        sink="clickhouse",
        backend=CDCBackend.MSSQL_CDC,
        source_schema="dbo",
        source_table="orders",
        target_dataset="analytics.orders_cdc",
        unique_key=("order_id",),
    )

    report = CdcRetentionGapService(policy=CdcRetentionPolicy(at_risk_margin=2)).evaluate(
        output_dir=tmp_path,
        stream=stream,
        committed_offset=CDCOffset(CDCBackend.MSSQL_CDC, "0x00000000000000000009", True),
        probe=StaticCdcRetentionProbe(
            CdcRetentionBounds(
                backend=CDCBackend.MSSQL_CDC,
                min_available_offset="0x0000000000000000000A",
                high_watermark="0x0000000000000000000F",
                current_offset="0x0000000000000000000F",
            )
        ),
    )

    assert report.passed is False
    assert report.metrics["offset_margin"] == -1
    assert report.decision.level == "gap_detected"
