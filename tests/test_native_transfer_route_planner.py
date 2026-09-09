from __future__ import annotations

import json
from pathlib import Path

from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy
from dpone.runtime.native_transfer_route_planner import (
    NativeTransferRouteCapabilityRegistry,
    NativeTransferRoutePlanner,
    RouteCertificationPolicy,
)
from dpone.runtime.native_transfer_route_registry import route_capability_hash


def test_certified_only_selects_certified_postgres_clickhouse_stream(tmp_path: Path) -> None:
    artifact = _route_artifact(tmp_path, source="postgres", sink="clickhouse", strategy="full_refresh")

    decision = NativeTransferRoutePlanner(NativeTransferRouteCapabilityRegistry()).plan(
        source_type="postgres",
        sink_type="clickhouse",
        strategy="full_refresh",
        source_options={"extract_mode": "copy_to_stdout"},
        sink_options={"clickhouse_bulk": {"mode": "http"}},
        execution_policy=NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "auto"}}),
        certification_policy=RouteCertificationPolicy(mode="certified_only", artifact=str(artifact)),
    )

    assert decision.selected_transport == "stream"
    assert decision.certification_status == "certified"
    assert decision.release_gate == "green"
    assert decision.fallback_chain == ("stream", "object", "file")
    assert decision.matrix.candidates[0].transport == "stream"
    assert decision.matrix.candidates[0].certified is True
    assert decision.to_evidence()["schema_version"] == "dpone.native_transfer.route_decision.v1"


def test_certified_only_blocks_uncertified_route_before_io(tmp_path: Path) -> None:
    missing = tmp_path / "missing-route-certification.json"

    decision = NativeTransferRoutePlanner(NativeTransferRouteCapabilityRegistry()).plan(
        source_type="postgres",
        sink_type="clickhouse",
        strategy="full_refresh",
        source_options={"extract_mode": "copy_to_stdout"},
        sink_options={"clickhouse_bulk": {"mode": "http"}},
        execution_policy=NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "auto"}}),
        certification_policy=RouteCertificationPolicy(mode="certified_only", artifact=str(missing)),
    )

    assert decision.selected_transport is None
    assert decision.release_gate == "blocked"
    assert "native_transfer_route_certification.missing" in decision.blockers
    assert decision.matrix.candidates[0].technically_eligible is True
    assert decision.matrix.candidates[0].certified is False


def test_certified_only_blocks_stale_route_certification_hash(tmp_path: Path) -> None:
    artifact = _route_artifact(
        tmp_path,
        source="postgres",
        sink="clickhouse",
        strategy="full_refresh",
        capability_hash="sha256:stale",
    )

    decision = NativeTransferRoutePlanner(NativeTransferRouteCapabilityRegistry()).plan(
        source_type="postgres",
        sink_type="clickhouse",
        strategy="full_refresh",
        source_options={"extract_mode": "copy_to_stdout"},
        sink_options={"clickhouse_bulk": {"mode": "http"}},
        execution_policy=NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "auto"}}),
        certification_policy=RouteCertificationPolicy(mode="certified_only", artifact=str(artifact)),
    )

    assert decision.selected_transport is None
    assert "native_transfer_route_certification.stale_hash" in decision.blockers


def test_certified_only_rejects_statusless_or_unverified_route_evidence(tmp_path: Path) -> None:
    for evidence_status in (None, "UNVERIFIED"):
        artifact = _route_artifact(
            tmp_path,
            source="postgres",
            sink="clickhouse",
            strategy="full_refresh",
            evidence_status=evidence_status,
        )

        decision = NativeTransferRoutePlanner(NativeTransferRouteCapabilityRegistry()).plan(
            source_type="postgres",
            sink_type="clickhouse",
            strategy="full_refresh",
            source_options={"extract_mode": "copy_to_stdout"},
            sink_options={"clickhouse_bulk": {"mode": "http"}},
            execution_policy=NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "auto"}}),
            certification_policy=RouteCertificationPolicy(mode="certified_only", artifact=str(artifact)),
        )

        assert decision.selected_transport is None
        assert "native_transfer_route_certification.blocked" in decision.blockers


def test_advisory_mode_allows_uncertified_mssql_file_fallback_with_reason() -> None:
    decision = NativeTransferRoutePlanner(NativeTransferRouteCapabilityRegistry()).plan(
        source_type="mssql",
        sink_type="clickhouse",
        strategy="full_refresh",
        source_options={"extract_mode": "bcp_queryout"},
        sink_options={"clickhouse_bulk": {"mode": "http"}},
        execution_policy=NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "auto"}}),
        certification_policy=RouteCertificationPolicy(mode="advisory"),
    )

    assert decision.selected_transport == "file"
    assert decision.certification_status == "uncertified"
    assert "native_transfer_route_uncertified_advisory" in decision.warnings
    assert "bcp_queryout_is_file_transport" in decision.reasons
    assert decision.to_transport_plan().transport == "file"
    assert decision.to_transport_plan().fallback_reason == "native_transfer_stream_fallback_file_only_source"


def test_forced_stream_blocks_mssql_bcp_route() -> None:
    decision = NativeTransferRoutePlanner(NativeTransferRouteCapabilityRegistry()).plan(
        source_type="mssql",
        sink_type="clickhouse",
        strategy="full_refresh",
        source_options={"extract_mode": "bcp_queryout"},
        sink_options={"clickhouse_bulk": {"mode": "http"}},
        execution_policy=NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "stream"}}),
        certification_policy=RouteCertificationPolicy(mode="advisory"),
    )

    assert decision.selected_transport is None
    assert decision.release_gate == "blocked"
    assert "native_transfer_forced_transport_unavailable:stream" in decision.blockers
    assert "bcp_queryout_is_file_transport" in decision.reasons


def _route_artifact(
    tmp_path: Path,
    *,
    source: str,
    sink: str,
    strategy: str,
    capability_hash: str | None = None,
    evidence_status: str | None = "PASS",
) -> Path:
    path = tmp_path / "native_transfer_route_certification.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "dpone.native_transfer.route_certification.v1",
                "passed": True,
                **({"evidence_status": evidence_status} if evidence_status is not None else {}),
                "status": "certified",
                "route": {"source": source, "sink": sink, "strategy": strategy},
                "certified_transports": ["stream"],
                "codec": "tabseparated",
                "capability_hash": capability_hash
                or route_capability_hash(source=source, sink=sink, strategy=strategy, transports=["stream"]),
                "blockers": [],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path
