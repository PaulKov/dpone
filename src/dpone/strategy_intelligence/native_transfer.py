"""Reusable native transfer planning models.

This module is intentionally pure: it has no connector imports and performs no
I/O. Runtime adapters consume the produced plan to choose existing source,
artifact, sink and state paths.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, cast

from dpone.runtime.bulk_options import BulkOptionsResolver, ClickHouseBulkOptionsResolver
from dpone.runtime.bulk_wire import BulkWirePlanner
from dpone.runtime.partitioning_options import PartitioningOptionsResolver
from dpone.strategy_intelligence.adaptive_partitioning import (
    AdaptivePartitioningOptions,
    AdaptivePartitioningService,
    PartitionRuntimeObservation,
)
from dpone.strategy_intelligence.evidence_contracts import NativeTransferEvidenceContractBuilder
from dpone.strategy_intelligence.native_snapshot_settings import build_snapshot_optimization_evidence
from dpone.strategy_intelligence.native_transfer_contracts import NativeTransferTransportContractBuilder
from dpone.strategy_intelligence.native_transfer_options import (
    default_merge_policy,
    has_canonical_load_workers,
    merge_options,
    schema_from_options,
)
from dpone.strategy_intelligence.native_transfer_type_fidelity import build_native_transfer_type_fidelity
from dpone.strategy_intelligence.replay_contracts import CdcReplayContractBuilder


@dataclass(frozen=True)
class NativeTransferRequest:
    """Input for native source-to-sink transfer planning."""

    source_type: str
    sink_type: str
    source_table: str
    target_table: str
    strategy: str
    unique_key: tuple[str, ...] = ()
    source_options: dict[str, Any] = field(default_factory=dict)
    sink_options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class NativeTransferPartitioningPlan:
    """Explainable partitioning decision for native transfer."""

    strategy: str
    column: str | None
    bounds_mode: str
    target_rows_per_partition: int | None
    max_partitions: int
    export_workers: int
    load_workers: int
    adaptive: dict[str, Any] = field(default_factory=lambda: {"enabled": False})

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NativeTransferPlan:
    """Explainable, vendor-neutral native transfer plan."""

    fast_path_id: str
    export_method: str
    ingest_method: str
    finalizer: str
    partitioning: NativeTransferPartitioningPlan
    native_ingest_settings: dict[str, Any]
    retry_boundaries: tuple[str, ...]
    transport_contract: dict[str, Any] = field(default_factory=dict)
    replay_contract: dict[str, Any] = field(default_factory=dict)
    evidence_contract: dict[str, Any] = field(default_factory=dict)
    type_fidelity: dict[str, Any] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()
    bottleneck_hints: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "fast_path_id": self.fast_path_id,
            "export_method": self.export_method,
            "ingest_method": self.ingest_method,
            "finalizer": self.finalizer,
            "partitioning": self.partitioning.to_dict(),
            "native_ingest_settings": dict(self.native_ingest_settings),
            "retry_boundaries": list(self.retry_boundaries),
            "transport_contract": dict(self.transport_contract),
            "replay_contract": dict(self.replay_contract),
            "evidence_contract": dict(self.evidence_contract),
            "type_fidelity": dict(self.type_fidelity),
            "warnings": list(self.warnings),
            "bottleneck_hints": list(self.bottleneck_hints),
        }


class NativeTransferPlanBuilder:
    """Build native transfer plans from manifest-normalized options."""

    _MSSQL_CLICKHOUSE_DIRECT_TSV_FAST_PATH = "mssql_bcp_queryout_to_clickhouse_direct_tsv"
    _MSSQL_CLICKHOUSE_TYPED_WIRE_FAST_PATH = "mssql_bcp_queryout_to_clickhouse_typed_wire"
    _MSSQL_CLICKHOUSE_TYPED_BINARY_FAST_PATH = "mssql_odbc_row_stream_to_clickhouse_rowbinary"
    _MSSQL_CLICKHOUSE_BCP_NATIVE_FAST_PATH = "mssql_bcp_native_to_clickhouse_rowbinary"
    _MSSQL_CLICKHOUSE_BCP_NATIVE_COLUMNAR_FAST_PATH = "mssql_bcp_native_to_clickhouse_native"
    _POSTGRES_MSSQL_FAST_PATH = "postgres_copy_to_mssql_bcp"
    _MYSQL_MSSQL_FAST_PATH = "mysql_stream_to_mssql_bcp"
    _RETRY_BOUNDARIES = ("export", "load_staging", "finalize")

    def build(self, request: NativeTransferRequest) -> NativeTransferPlan:
        source = request.source_type.lower()
        sink = request.sink_type.lower()
        warnings: list[str] = []

        partitioning = self._build_partitioning_plan(request, warnings)
        native_ingest_settings = self._native_ingest_settings(request, warnings)

        if source == "mssql" and sink == "clickhouse":
            bulk_wire = BulkWirePlanner().plan(
                source_type=source,
                sink_type=sink,
                schema=tuple(schema_from_options(request.source_options)),
                source_options=request.source_options,
                sink_options=request.sink_options,
            )
            if bulk_wire.selected_route == "typed_binary_bcp_native":
                fast_path_id = (
                    self._MSSQL_CLICKHOUSE_BCP_NATIVE_COLUMNAR_FAST_PATH
                    if bulk_wire.input_format == "Native"
                    else self._MSSQL_CLICKHOUSE_BCP_NATIVE_FAST_PATH
                )
                export_method = "bcp_queryout_native"
                ingest_method = (
                    "clickhouse_native_staging"
                    if bulk_wire.input_format == "Native"
                    else "clickhouse_rowbinary_staging"
                )
            elif bulk_wire.selected_route == "typed_binary_row_stream":
                fast_path_id = self._MSSQL_CLICKHOUSE_TYPED_BINARY_FAST_PATH
                export_method = request.source_options.get("mssql_export_mode") or "row_stream"
                ingest_method = "clickhouse_rowbinary_staging"
            elif bulk_wire.selected_route == "typed_raw_direct":
                fast_path_id = self._MSSQL_CLICKHOUSE_TYPED_WIRE_FAST_PATH
                export_method = request.source_options.get("extract_mode") or "bcp_queryout"
                ingest_method = "clickhouse_typed_staging"
            else:
                fast_path_id = self._MSSQL_CLICKHOUSE_DIRECT_TSV_FAST_PATH
                export_method = request.source_options.get("extract_mode") or "bcp_queryout"
                ingest_method = "clickhouse_direct_tsv"
        elif source == "postgres" and sink == "mssql":
            fast_path_id = self._POSTGRES_MSSQL_FAST_PATH
            export_method = request.source_options.get("extract_mode") or "copy_to_stdout"
            ingest_method = "mssql_bcp"
        elif source == "mysql" and sink == "mssql":
            fast_path_id = self._MYSQL_MSSQL_FAST_PATH
            export_method = request.source_options.get("extract_mode") or "streaming_select"
            ingest_method = "mssql_bcp"
        else:
            fast_path_id = f"{source}_to_{sink}_native_plan"
            export_method = request.source_options.get("extract_mode") or "streaming"
            ingest_method = self._bulk_mode(request.sink_options) or "native_sink"

        type_fidelity = self._type_fidelity(request)
        warnings.extend(str(warning) for warning in type_fidelity.get("warnings", ()))
        transport_contract = NativeTransferTransportContractBuilder().build(
            source_type=request.source_type,
            sink_type=request.sink_type,
            source_options=request.source_options,
            native_ingest_settings=native_ingest_settings,
        )
        if transport_contract is not None:
            warnings.extend(transport_contract.warnings)
        replay_contract = CdcReplayContractBuilder().build(
            source_type=request.source_type,
            sink_type=request.sink_type,
            strategy=request.strategy,
            unique_key=request.unique_key,
            source_options=request.source_options,
            sink_options=request.sink_options,
        )
        if replay_contract is not None:
            warnings.extend(replay_contract.warnings)
        evidence_contract = NativeTransferEvidenceContractBuilder().build(
            source_type=request.source_type,
            sink_type=request.sink_type,
            strategy=request.strategy,
            partitioning=cast(Any, partitioning),
        )

        return NativeTransferPlan(
            fast_path_id=fast_path_id,
            export_method=str(export_method),
            ingest_method=str(ingest_method),
            finalizer=self._resolve_finalizer(request),
            partitioning=partitioning,
            native_ingest_settings=native_ingest_settings,
            retry_boundaries=self._RETRY_BOUNDARIES,
            transport_contract=transport_contract.to_dict() if transport_contract else {},
            replay_contract=replay_contract.to_dict() if replay_contract else {},
            evidence_contract=evidence_contract.to_dict(),
            type_fidelity=type_fidelity,
            warnings=tuple(warnings),
            bottleneck_hints=self._bottleneck_hints(request, partitioning),
        )

    def _build_partitioning_plan(
        self,
        request: NativeTransferRequest,
        warnings: list[str],
    ) -> NativeTransferPartitioningPlan:
        resolved = PartitioningOptionsResolver.resolve(request.source_options)
        load_workers = resolved.load_workers
        if request.sink_options.get("parallel_load_workers") is not None and not has_canonical_load_workers(
            request.source_options
        ):
            load_workers = int(request.sink_options["parallel_load_workers"])
            warnings.append(
                "sink.options.parallel_load_workers is deprecated; use source.options.partitioning.load_workers."
            )
        warnings.extend(resolved.warnings)

        column = resolved.column
        if not column:
            warnings.append("No safe partition column configured; using a single partition.")
            return NativeTransferPartitioningPlan(
                strategy="single",
                column=None,
                bounds_mode="none",
                target_rows_per_partition=None,
                max_partitions=1,
                export_workers=1,
                load_workers=1,
            )

        adaptive = self._adaptive_partitioning(request, resolved)
        warnings.extend(str(warning) for warning in adaptive.get("warnings", ()))
        return NativeTransferPartitioningPlan(
            strategy=resolved.strategy,
            column=str(column),
            bounds_mode=resolved.bounds_mode,
            target_rows_per_partition=resolved.target_rows_per_partition,
            max_partitions=max(1, resolved.max_partitions),
            export_workers=max(1, resolved.export_workers),
            load_workers=max(1, load_workers),
            adaptive=adaptive,
        )

    def _native_ingest_settings(self, request: NativeTransferRequest, warnings: list[str]) -> dict[str, Any]:
        sink = request.sink_type.lower()
        source = request.source_type.lower()
        settings: dict[str, Any] = {}
        if source == "mssql":
            bulk = BulkOptionsResolver.resolve(merge_options(request.sink_options, request.source_options))
            warnings.extend(bulk.warnings)
            settings["bulk"] = bulk.to_dict()
        if sink == "clickhouse":
            clickhouse = ClickHouseBulkOptionsResolver.resolve(
                merge_options(request.source_options, request.sink_options)
            )
            warnings.extend(clickhouse.warnings)
            settings["clickhouse_bulk"] = clickhouse.to_dict()
            if source == "mssql":
                settings["bulk_wire"] = self._bulk_wire_evidence(request, source, sink)
                settings["snapshot_optimization"] = build_snapshot_optimization_evidence(
                    source_type=source,
                    sink_type=sink,
                    source_options=request.source_options,
                    sink_options=request.sink_options,
                )
            return settings
        if sink == "mssql":
            bulk = BulkOptionsResolver.resolve(request.sink_options)
            warnings.extend(bulk.warnings)
            settings["bulk"] = bulk.to_dict()
            return settings
        if sink == "kafka":
            settings["delivery"] = dict(request.sink_options.get("delivery") or {})
            return settings
        return settings

    def _resolve_finalizer(self, request: NativeTransferRequest) -> str:
        sink = request.sink_type.lower()
        if request.strategy == "incremental_merge":
            return str(request.sink_options.get("merge_policy") or default_merge_policy(sink))
        if request.strategy == "full_refresh":
            return "shadow_swap"
        if request.strategy == "incremental_append":
            return "append"
        if request.strategy == "partition_replace":
            return "replace_partition"
        if request.strategy == "cdc_apply":
            return "cdc_apply"
        return request.strategy

    def _bottleneck_hints(
        self,
        request: NativeTransferRequest,
        partitioning: NativeTransferPartitioningPlan,
    ) -> tuple[str, ...]:
        hints: list[str] = []
        if request.source_type.lower() == "mssql" and partitioning.strategy == "single":
            hints.append("MSSQL export is single-partition; configure partitioning.column for parallel bcp queryout.")
        if (
            request.sink_type.lower() == "clickhouse"
            and not ClickHouseBulkOptionsResolver.resolve(request.sink_options).insert_settings
        ):
            hints.append(
                "ClickHouse insert settings are default; tune async_insert and block size for large transfers."
            )
        if request.source_type.lower() == "mssql" and request.sink_type.lower() == "clickhouse":
            bulk_wire = BulkWirePlanner().plan(
                source_type=request.source_type,
                sink_type=request.sink_type,
                schema=tuple(schema_from_options(request.source_options)),
                source_options=request.source_options,
                sink_options=request.sink_options,
            )
            if (
                bulk_wire.input_format == "Native"
                and bulk_wire.acceleration.selected_backend == "python_reference"
                and "native_acceleration_disabled" in bulk_wire.acceleration.warning_codes
            ):
                hints.append("Native acceleration is disabled; Python reference transcode will cap throughput.")
            elif bulk_wire.input_format == "Native" and bulk_wire.acceleration.selected_backend == "python_reference":
                hints.append(
                    "Native acceleration is unavailable; install dpone[accel] or use required mode to fail fast."
                )
        return tuple(hints)

    def _bulk_mode(self, sink_options: dict[str, Any]) -> str | None:
        bulk = BulkOptionsResolver.resolve(sink_options, default_mode="")
        return bulk.mode or None

    def _bulk_wire_evidence(self, request: NativeTransferRequest, source: str, sink: str) -> dict[str, Any]:
        return (
            BulkWirePlanner()
            .plan(
                source_type=source,
                sink_type=sink,
                schema=tuple(schema_from_options(request.source_options)),
                source_options=request.source_options,
                sink_options=request.sink_options,
            )
            .to_evidence()
        )

    def _type_fidelity(self, request: NativeTransferRequest) -> dict[str, Any]:
        return build_native_transfer_type_fidelity(
            source_type=request.source_type,
            sink_type=request.sink_type,
            source_options=request.source_options,
            sink_options=request.sink_options,
        )

    def _adaptive_partitioning(self, request: NativeTransferRequest, resolved: Any) -> dict[str, Any]:
        nested = request.source_options.get("partitioning")
        raw_adaptive = nested.get("adaptive") if isinstance(nested, dict) else None
        adaptive: dict[str, Any] = dict(raw_adaptive) if isinstance(raw_adaptive, dict) else {}
        defaults = {
            "target_rows_per_partition": resolved.target_rows_per_partition,
            "max_partitions": resolved.max_partitions,
            "export_workers": resolved.export_workers,
            "load_workers": resolved.load_workers,
        }
        options = AdaptivePartitioningOptions.from_mapping(adaptive, defaults=defaults)
        observations = tuple(
            PartitionRuntimeObservation.from_mapping(item)
            for item in adaptive.get("observations", ())
            if isinstance(item, dict)
        )
        return AdaptivePartitioningService().plan(options=options, observations=observations).to_dict()
