from __future__ import annotations

from dpone.connector_sdk.native_transfer_certification import (
    ConnectorCapabilityCertificationService,
    NativeTransferCapability,
    TransportCapabilityMatrix,
)
from dpone.runtime.native_transfer_evidence import NativeTransferTransportEvidenceBuilder
from dpone.runtime.native_transfer_transport import SliceTransportPlan, StreamEligibility


def test_transport_capability_matrix_selects_stream_when_source_sink_and_codec_match() -> None:
    matrix = TransportCapabilityMatrix.from_capabilities(
        source=NativeTransferCapability.stream_export(formats=("tabseparated",)),
        sink=NativeTransferCapability.stream_staging_load(formats=("tabseparated",)),
        codec_format="tabseparated",
    )

    assert matrix.selected_transport == "stream"
    assert matrix.source_supported is True
    assert matrix.sink_supported is True
    assert matrix.codec_supported is True
    assert matrix.fallback_reason is None


def test_transport_capability_matrix_records_file_fallback_for_file_only_source() -> None:
    matrix = TransportCapabilityMatrix.from_capabilities(
        source=NativeTransferCapability.file_export(),
        sink=NativeTransferCapability.stream_staging_load(formats=("tabseparated",)),
        codec_format="tabseparated",
    )

    assert matrix.selected_transport == "file"
    assert matrix.fallback_reason == "native_transfer_stream_fallback_file_only_source"
    assert "source lacks stream_export" in matrix.reasons


def test_connector_capability_certification_blocks_missing_declared_stream_capability() -> None:
    report = ConnectorCapabilityCertificationService().certify(
        manifest={
            "connector": "warehouse_db",
            "connector_type": "database",
            "native_transfer": {"capabilities": {"file_export": {"supports_cleanup": True}}},
        },
        profile="static",
        requested_capabilities=("native_transfer.stream",),
    )

    assert report.status == "blocked"
    assert report.capabilities["native_transfer.stream"].status == "blocked"
    assert "native_transfer_stream_capability_missing" in report.blockers


def test_connector_capability_certification_keeps_declarative_live_profile_unverified() -> None:
    report = ConnectorCapabilityCertificationService().certify(
        manifest={
            "connector": "warehouse_db",
            "connector_type": "database",
            "native_transfer": {
                "capabilities": {
                    "stream_export": {
                        "formats": ["tabseparated", "jsonl"],
                        "bounded": True,
                        "supports_checksum": True,
                        "supports_cleanup": True,
                    },
                    "stream_staging_load": {
                        "formats": ["tabseparated", "jsonl"],
                        "staging_safe": True,
                        "supports_abort": True,
                        "supports_idempotency_key": True,
                    },
                }
            },
        },
        profile="local_live",
        requested_capabilities=("native_transfer.stream",),
    )

    assert report.passed is False
    assert report.status == "unverified"
    assert report.capabilities["native_transfer.stream"].status == "unverified"
    assert "live_evidence_not_provided:local_live" in report.blockers
    assert {case.name for case in report.capabilities["native_transfer.stream"].cases} == {
        "stream_success",
        "file_fallback",
        "source_failure",
        "sink_failure",
        "timeout",
        "checksum_mismatch",
    }
    assert {case.status for case in report.capabilities["native_transfer.stream"].cases} == {"unverified"}


def test_connector_capability_certification_keeps_plan_only_static_profile_unverified() -> None:
    report = ConnectorCapabilityCertificationService().certify(
        manifest={
            "connector": "warehouse_db",
            "connector_type": "database",
            "native_transfer": {
                "capabilities": {
                    "stream_export": {
                        "formats": ["tabseparated"],
                        "bounded": True,
                        "supports_cleanup": True,
                    },
                    "stream_staging_load": {
                        "formats": ["tabseparated"],
                        "staging_safe": True,
                        "supports_abort": True,
                    },
                }
            },
        },
        profile="static",
        requested_capabilities=("native_transfer.stream",),
    )

    assert report.passed is False
    assert report.status == "unverified"
    assert report.blockers == ("static_profile_is_plan_only",)
    assert {case.status for case in report.capabilities["native_transfer.stream"].cases} == {"unverified"}


def test_connector_capability_certification_blocks_unknown_profile() -> None:
    report = ConnectorCapabilityCertificationService().certify(
        manifest={"connector": "warehouse_db", "connector_type": "database"},
        profile="typo",
        requested_capabilities=("native_transfer.stream",),
    )

    assert report.passed is False
    assert report.status == "blocked"
    assert report.profile == "typo"
    assert report.blockers == ("unsupported_certification_profile:typo",)


def test_connector_capability_certification_does_not_pass_empty_request() -> None:
    report = ConnectorCapabilityCertificationService().certify(
        manifest={"connector": "warehouse_db", "connector_type": "database"},
        profile="static",
        requested_capabilities=(),
    )

    assert report.passed is False
    assert report.status == "unverified"
    assert report.blockers == ("requested_capabilities_missing",)


def test_native_transfer_transport_evidence_contract_redacts_details() -> None:
    plan = SliceTransportPlan(
        transport="file",
        eligibility=StreamEligibility(
            source=False,
            sink=True,
            codec=True,
            reasons=("bcp_queryout_is_file_transport",),
        ),
        fallback_allowed=True,
        fallback_reason="native_transfer_stream_fallback_file_only_source",
    )

    evidence = NativeTransferTransportEvidenceBuilder().build(
        plan,
        requested_transport="auto",
        rows=100,
        bytes_count=2048,
        checksum="sha256:abc",
        duration_seconds=1.25,
        staging_session_id="stg_123",
        cleanup_status="eager_deleted",
        failure_code=None,
        diagnostics={"password": "secret", "safe": "kept"},
    )

    payload = evidence.to_dict()

    assert payload["schema_version"] == "dpone.native_transfer.transport_evidence.v1"
    assert payload["selected_transport"] == "file"
    assert payload["requested_transport"] == "auto"
    assert payload["fallback_reason"] == "native_transfer_stream_fallback_file_only_source"
    assert payload["failure_code"] is None
    assert payload["metrics"]["rows"] == 100
    assert payload["diagnostics"] == {"password": "***", "safe": "kept"}
