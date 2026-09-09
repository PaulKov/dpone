"""Connector-neutral source-native to sink-binary transcoding."""

from __future__ import annotations

from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import Any

from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.clickhouse_native import ClickHouseNativeEncoder
from dpone.runtime.clickhouse_rowbinary import ClickHouseRowBinaryEncoder
from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.native_acceleration import (
    NATIVE_ACCELERATED_BACKEND,
    NativeAccelerationEvidence,
    NativeAccelerationPolicy,
    NativeAccelerationRegistry,
)
from dpone.runtime.native_wire_models import NativeWireEvidence
from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder
from dpone.runtime.native_wire_transcode_support import (
    attach_stream_contracts,
    build_acceleration_policy,
    bytes_value,
)
from dpone.runtime.sink_binary import SinkBinaryEvidence


class NativeWireTranscoder:
    """Transcode source-native artifacts into sink binary byte streams."""

    def __init__(self, registry: NativeAccelerationRegistry | None = None) -> None:
        self._registry = registry or NativeAccelerationRegistry()

    def to_clickhouse_binary(
        self,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
        *,
        clickhouse_schema: Sequence[tuple[str, str]] | None = None,
        type_policy: Any | None = None,
    ) -> ByteStreamArtifact:
        contract = artifact.native_wire_contract
        if contract.target_format == "Native":
            return self.to_clickhouse_native(
                artifact,
                schema,
                clickhouse_schema=clickhouse_schema,
                type_policy=type_policy,
            )
        return self.to_clickhouse_rowbinary(
            artifact,
            schema,
            clickhouse_schema=clickhouse_schema,
            type_policy=type_policy,
        )

    def to_clickhouse_rowbinary(
        self,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
        *,
        clickhouse_schema: Sequence[tuple[str, str]] | None = None,
        type_policy: Any | None = None,
    ) -> ByteStreamArtifact:
        contract = artifact.native_wire_contract
        if contract.source_format != "mssql-bcp-native":
            raise ValueError(f"native_wire_unsupported_source_format:{contract.source_format}")
        if contract.target_format != "RowBinary":
            raise ValueError(f"native_wire_unsupported_target_format:{contract.target_format}")

        decoder = MssqlBcpNativeDecoder(contract)
        encoder = ClickHouseRowBinaryEncoder(
            schema,
            target_schema=clickhouse_schema,
            type_policy=type_policy,
        )
        evidence = NativeWireEvidence(
            source_format=contract.source_format,
            target_format=contract.target_format,
            schema_hash=contract.schema_hash,
            query_hash=contract.query_hash,
            type_layout_hash=contract.type_layout_hash,
            decoded_bytes=Path(artifact.file_path).stat().st_size,
        )
        sink_evidence = SinkBinaryEvidence(format="rowbinary", input_format="RowBinary")
        acceleration_evidence = NativeAccelerationEvidence.from_decision(
            self._registry.decide(
                policy=NativeAccelerationPolicy(mode="off"),
                source_format=contract.source_format,
                target_format=contract.target_format,
                source_types=tuple(column.source_type for column in contract.columns),
            )
        )

        def chunks():
            digest = sha256()
            rows = 0
            started_at = perf_counter()

            def counted_rows():
                nonlocal rows
                for row in decoder.iter_rows(artifact.file_path):
                    rows += 1
                    yield row

            try:
                for batch in encoder.iter_batches(counted_rows()):
                    evidence.encoded_bytes += len(batch)
                    sink_evidence.block_count += 1
                    sink_evidence.encoded_bytes += len(batch)
                    digest.update(batch)
                    yield batch
                evidence.rows = rows
                sink_evidence.rows = rows
                evidence.checksum = "sha256:" + digest.hexdigest()
                acceleration_evidence.finish(
                    rows=rows,
                    decoded_bytes=evidence.decoded_bytes,
                    encoded_bytes=evidence.encoded_bytes,
                    block_count=sink_evidence.block_count,
                    started_at=started_at,
                )
            except Exception:
                evidence.failure_code = "native_wire_transcode_failed"
                sink_evidence.failure_code = "sink_binary_encode_failed"
                acceleration_evidence.failure_code = "native_acceleration_python_reference_failed"
                raise

        stream = ByteStreamArtifact(
            chunks,
            columns=artifact.columns,
            format="clickhouse-rowbinary",
            estimated_rows=artifact.estimated_rows,
            cleanup_callback=artifact.cleanup,
        )
        attach_stream_contracts(
            stream,
            bulk_wire_contract=artifact.bulk_wire_contract
            or SimpleNamespace(
                input_format="RowBinary",
                delimiter_profile=SimpleNamespace(clickhouse_settings={}),
                selected_route="typed_binary_bcp_native",
            ),
            native_wire_evidence=evidence,
            sink_binary_evidence=sink_evidence,
            native_acceleration_evidence=acceleration_evidence,
        )
        return stream

    def to_clickhouse_native(
        self,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
        *,
        clickhouse_schema: Sequence[tuple[str, str]] | None = None,
        type_policy: Any | None = None,
    ) -> ByteStreamArtifact:
        contract = artifact.native_wire_contract
        if contract.source_format != "mssql-bcp-native":
            raise ValueError(f"native_wire_unsupported_source_format:{contract.source_format}")
        if contract.target_format != "Native":
            raise ValueError(f"native_wire_unsupported_target_format:{contract.target_format}")

        bulk_wire = getattr(artifact, "bulk_wire_contract", None)
        acceleration_policy = build_acceleration_policy(bulk_wire)
        acceleration_decision = self._registry.decide(
            policy=acceleration_policy,
            source_format=contract.source_format,
            target_format=contract.target_format,
            source_types=tuple(column.source_type for column in contract.columns),
        )
        publish_runtime_decision(
            acceleration_decision,
            decision_id="native_transfer.acceleration",
            phase="load",
            component="native_wire_transcoder",
            category="backend_selection",
            fallback_allowed=acceleration_policy.mode == "auto" and not acceleration_decision.blocked,
            provider="dpone-native-accel",
            details={
                "source_format": contract.source_format,
                "target_format": contract.target_format,
                "schema_hash": contract.schema_hash,
                "type_layout_hash": contract.type_layout_hash,
                "source_type_count": len(contract.columns),
            },
        )
        if acceleration_decision.blocked:
            raise ValueError(";".join(acceleration_decision.blocker_codes))
        if acceleration_decision.selected_backend == NATIVE_ACCELERATED_BACKEND:
            return self._to_clickhouse_native_accelerated(
                artifact,
                schema,
                clickhouse_schema=clickhouse_schema,
                type_policy=type_policy,
                decision=acceleration_decision,
            )

        decoder = MssqlBcpNativeDecoder(contract)
        encoder = ClickHouseNativeEncoder(
            schema,
            target_schema=clickhouse_schema,
            type_policy=type_policy,
            block_rows=int(getattr(bulk_wire, "block_rows", 65_536)),
            block_bytes=bytes_value(getattr(bulk_wire, "block_bytes", None)),
        )
        evidence = NativeWireEvidence(
            source_format=contract.source_format,
            target_format=contract.target_format,
            schema_hash=contract.schema_hash,
            query_hash=contract.query_hash,
            type_layout_hash=contract.type_layout_hash,
            decoded_bytes=Path(artifact.file_path).stat().st_size,
        )
        sink_evidence = SinkBinaryEvidence(format="native", input_format="Native")
        acceleration_evidence = NativeAccelerationEvidence.from_decision(acceleration_decision)

        def chunks():
            digest = sha256()
            rows = 0
            started_at = perf_counter()

            def counted_rows():
                nonlocal rows
                for row in decoder.iter_rows(artifact.file_path):
                    rows += 1
                    yield row

            try:
                for batch in encoder.iter_batches(counted_rows()):
                    evidence.encoded_bytes += len(batch)
                    sink_evidence.block_count += 1
                    sink_evidence.encoded_bytes += len(batch)
                    digest.update(batch)
                    yield batch
                evidence.rows = rows
                sink_evidence.rows = rows
                evidence.checksum = "sha256:" + digest.hexdigest()
                acceleration_evidence.finish(
                    rows=rows,
                    decoded_bytes=evidence.decoded_bytes,
                    encoded_bytes=evidence.encoded_bytes,
                    block_count=sink_evidence.block_count,
                    started_at=started_at,
                )
            except Exception:
                evidence.failure_code = "native_wire_transcode_failed"
                sink_evidence.failure_code = "sink_binary_encode_failed"
                acceleration_evidence.failure_code = "native_acceleration_python_reference_failed"
                raise

        stream = ByteStreamArtifact(
            chunks,
            columns=artifact.columns,
            format="clickhouse-native",
            estimated_rows=artifact.estimated_rows,
            cleanup_callback=artifact.cleanup,
        )
        attach_stream_contracts(
            stream,
            bulk_wire_contract=artifact.bulk_wire_contract
            or SimpleNamespace(
                input_format="Native",
                delimiter_profile=SimpleNamespace(clickhouse_settings={}),
                selected_route="typed_binary_bcp_native",
                binary_format="native",
                block_rows=65_536,
                block_bytes=None,
            ),
            native_wire_evidence=evidence,
            sink_binary_evidence=sink_evidence,
            native_acceleration_evidence=acceleration_evidence,
        )
        return stream

    def _to_clickhouse_native_accelerated(
        self,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
        *,
        clickhouse_schema: Sequence[tuple[str, str]] | None,
        type_policy: Any | None,
        decision: Any,
    ) -> ByteStreamArtifact:
        contract = artifact.native_wire_contract
        evidence = NativeWireEvidence(
            source_format=contract.source_format,
            target_format=contract.target_format,
            schema_hash=contract.schema_hash,
            query_hash=contract.query_hash,
            type_layout_hash=contract.type_layout_hash,
            decoded_bytes=Path(artifact.file_path).stat().st_size,
        )
        sink_evidence = SinkBinaryEvidence(format="native", input_format="Native")
        acceleration_evidence = NativeAccelerationEvidence.from_decision(decision)

        def chunks():
            digest = sha256()
            encoded_bytes = 0
            block_count = 0
            rows = 0
            started_at = perf_counter()
            try:
                type_policy_to_dict = getattr(type_policy, "to_dict", None)
                request = {
                    "artifact_path": str(artifact.file_path),
                    "schema": list(schema),
                    "clickhouse_schema": list(clickhouse_schema or ()),
                    "type_policy": type_policy_to_dict() if callable(type_policy_to_dict) else None,
                    "native_wire_contract": contract.to_dict(),
                    "bulk_wire_contract": getattr(artifact.bulk_wire_contract, "to_dict", lambda: {})(),
                }
                for batch in self._registry.transcode_batches(decision=decision, request=request):
                    payload = batch.payload
                    encoded_bytes += len(payload)
                    block_count += 1
                    rows += batch.rows
                    digest.update(payload)
                    yield payload
                evidence.encoded_bytes = encoded_bytes
                evidence.rows = rows
                evidence.checksum = "sha256:" + digest.hexdigest()
                sink_evidence.encoded_bytes = encoded_bytes
                sink_evidence.block_count = block_count
                sink_evidence.rows = evidence.rows
                acceleration_evidence.finish(
                    rows=evidence.rows,
                    decoded_bytes=evidence.decoded_bytes,
                    encoded_bytes=encoded_bytes,
                    block_count=block_count,
                    started_at=started_at,
                )
            except Exception:
                evidence.failure_code = "native_wire_transcode_failed"
                sink_evidence.failure_code = "sink_binary_encode_failed"
                acceleration_evidence.failure_code = "native_acceleration_backend_failed"
                raise

        stream = ByteStreamArtifact(
            chunks,
            columns=artifact.columns,
            format="clickhouse-native",
            estimated_rows=artifact.estimated_rows,
            cleanup_callback=artifact.cleanup,
        )
        attach_stream_contracts(
            stream,
            bulk_wire_contract=artifact.bulk_wire_contract,
            native_wire_evidence=evidence,
            sink_binary_evidence=sink_evidence,
            native_acceleration_evidence=acceleration_evidence,
        )
        return stream


__all__ = ["NativeWireTranscoder"]
