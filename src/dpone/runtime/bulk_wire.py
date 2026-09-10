"""Typed native bulk wire contracts for connector-neutral transfer planning."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from hashlib import sha256
from typing import Any

from dpone.runtime.native_acceleration import (
    NativeAccelerationDecision,
    NativeAccelerationPolicy,
    NativeAccelerationRegistry,
)

BULK_WIRE_SCHEMA_VERSION = "dpone.native_transfer.bulk_wire.v1"


@dataclass(frozen=True, slots=True)
class BulkWirePolicy:
    """User-facing native transfer wire policy."""

    mode: str = "auto"
    text_safety: str = "prove_or_transcode"
    delimiter_profile: str = "auto"
    null_policy: str = "sidecar"
    binary_format: str = "rowbinary"
    source_native_format: str = "auto"
    block_rows: int = 65_536
    block_bytes: str | None = None
    acceleration: NativeAccelerationPolicy = field(default_factory=NativeAccelerationPolicy)

    @classmethod
    def from_options(cls, options: Mapping[str, Any] | None) -> BulkWirePolicy:
        native = _mapping((options or {}).get("native_transfer"))
        wire = _mapping(native.get("wire"))
        return cls(
            mode=str(wire.get("mode") or "auto").strip().lower(),
            text_safety=str(wire.get("text_safety") or "prove_or_transcode").strip().lower(),
            delimiter_profile=str(wire.get("delimiter_profile") or "auto").strip().lower(),
            null_policy=str(wire.get("null_policy") or "sidecar").strip().lower(),
            binary_format=str(wire.get("binary_format") or "rowbinary").strip().lower(),
            source_native_format=str(wire.get("source_native_format") or "auto").strip().lower(),
            block_rows=int(wire.get("block_rows") or 65_536),
            block_bytes=str(wire["block_bytes"]).strip() if wire.get("block_bytes") is not None else None,
            acceleration=NativeAccelerationPolicy.from_mapping(_mapping(wire.get("acceleration"))),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BulkWireDelimiterProfile:
    """Physical delimiters and ClickHouse CustomSeparated settings."""

    name: str
    field_delimiter: str
    row_after_delimiter: str
    clickhouse_format: str
    row_between_delimiter: str = ""
    source_row_terminator: str | None = None
    clickhouse_settings: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "field_delimiter": self.field_delimiter,
            "row_after_delimiter": self.row_after_delimiter,
            "row_between_delimiter": self.row_between_delimiter,
            "source_row_terminator": self.source_row_terminator or self.row_after_delimiter,
            "clickhouse_format": self.clickhouse_format,
            "clickhouse_settings": dict(self.clickhouse_settings),
        }


@dataclass(frozen=True, slots=True)
class BulkWireColumn:
    """Column-level wire mapping."""

    name: str
    source_type: str
    target_type: str
    nullable: bool
    wire_type: str
    sidecar_columns: tuple[str, ...] = ()
    decode_expression_id: str = "identity"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BulkWireContract:
    """Resolved typed wire contract shared by source, sink, and evidence."""

    schema_version: str
    selected_route: str
    requested_mode: str
    source_type: str
    sink_type: str
    input_format: str
    binary_format: str
    block_rows: int
    block_bytes: str | None
    delimiter_profile: BulkWireDelimiterProfile
    columns: tuple[BulkWireColumn, ...]
    schema_hash: str
    source_escaping: bool
    acceleration: NativeAccelerationDecision
    warnings: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "selected_route": self.selected_route,
            "requested_mode": self.requested_mode,
            "source_type": self.source_type,
            "sink_type": self.sink_type,
            "input_format": self.input_format,
            "binary_format": self.binary_format,
            "block_rows": self.block_rows,
            "block_bytes": self.block_bytes,
            "delimiter_profile": self.delimiter_profile.to_dict(),
            "columns": [column.to_dict() for column in self.columns],
            "schema_hash": self.schema_hash,
            "source_escaping": self.source_escaping,
            "acceleration": self.acceleration.to_dict(),
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
        }

    def to_evidence(self) -> dict[str, Any]:
        """Return stable plan/runtime evidence without exposing data values."""

        return {
            "schema_version": self.schema_version,
            "selected_route": self.selected_route,
            "requested_mode": self.requested_mode,
            "source_type": self.source_type,
            "sink_type": self.sink_type,
            "input_format": self.input_format,
            "binary_format": self.binary_format,
            "block_rows": self.block_rows,
            "block_bytes": self.block_bytes,
            "delimiter_profile": self.delimiter_profile.to_dict(),
            "schema_hash": self.schema_hash,
            "mssql_source_escaping": self.source_escaping if self.source_type == "mssql" else None,
            "acceleration": self.acceleration.to_dict(),
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
        }


class BulkWirePlanner:
    """Pure typed-wire route decision service."""

    def plan(
        self,
        *,
        source_type: str,
        sink_type: str,
        schema: Sequence[tuple[str, str]],
        source_options: Mapping[str, Any] | None,
        sink_options: Mapping[str, Any] | None,
    ) -> BulkWireContract:
        policy = BulkWirePolicy.from_options(source_options)
        ingest_contract = _ingest_contract(sink_options)
        route = _selected_route(policy, ingest_contract)
        profile = _delimiter_profile(
            "binary" if route in {"typed_binary_row_stream", "typed_binary_bcp_native"} else policy.delimiter_profile,
            binary_format=policy.binary_format,
        )
        if policy.binary_format == "mssql_native":
            if (source_type.lower(), sink_type.lower(), policy.mode) != ("clickhouse", "mssql", "typed_binary"):
                raise ValueError("mssql_native.route_unsupported")
            route = "typed_binary_mssql_native"
            profile = BulkWireDelimiterProfile("binary", "", "", "MSSQLNative")
        columns = tuple(_column(name, dtype) for name, dtype in schema)
        acceleration = NativeAccelerationRegistry().decide(
            policy=policy.acceleration,
            source_format="mssql-bcp-native" if route == "typed_binary_bcp_native" else source_type,
            target_format=profile.clickhouse_format,
            source_types=tuple(str(dtype) for _, dtype in schema),
        )
        warnings = _warnings(route, policy, profile)
        if route == "typed_binary_mssql_native":
            warnings += ("mssql_native_requires_composed_source_and_target_authority",)
        return BulkWireContract(
            schema_version=BULK_WIRE_SCHEMA_VERSION,
            selected_route=route,
            requested_mode=policy.mode,
            source_type=_normalize(source_type),
            sink_type=_normalize(sink_type),
            input_format=profile.clickhouse_format,
            binary_format=_binary_format(policy.binary_format),
            block_rows=max(1, policy.block_rows),
            block_bytes=policy.block_bytes,
            delimiter_profile=profile,
            columns=columns,
            schema_hash=_schema_hash(schema),
            source_escaping=route == "source_encoded_direct_tsv",
            acceleration=acceleration,
            warnings=warnings,
        )


def should_use_source_encoded_tsv(options: Mapping[str, Any] | None) -> bool:
    """Return whether source SQL should emit ClickHouse-ready escaped TSV."""

    policy = BulkWirePolicy.from_options(options)
    ingest_contract = _ingest_contract(options)
    if policy.mode == "source_encoded" or ingest_contract == "direct_tsv":
        return True
    if policy.mode in {"typed_binary", "binary", "typed_raw", "driver"}:
        return False
    if ingest_contract in {"typed_binary_staging", "typed_staging", "typed_raw_streaming_staging"}:
        return False
    return True


def _selected_route(policy: BulkWirePolicy, ingest_contract: str) -> str:
    if policy.mode in {"typed_binary", "binary"} and policy.source_native_format in {"bcp_native", "bcp-native"}:
        return "typed_binary_bcp_native"
    if policy.mode in {"typed_binary", "binary"} or ingest_contract == "typed_binary_staging":
        return "typed_binary_row_stream"
    if policy.mode == "source_encoded" or ingest_contract == "direct_tsv":
        return "source_encoded_direct_tsv"
    if policy.mode == "driver" or ingest_contract == "python":
        return "driver"
    if policy.mode == "typed_raw" or ingest_contract in {"typed_staging", "typed_raw_streaming_staging"}:
        return "typed_raw_direct"
    return "source_encoded_direct_tsv"


def _delimiter_profile(value: str, *, binary_format: str = "rowbinary") -> BulkWireDelimiterProfile:
    if value == "binary":
        return BulkWireDelimiterProfile(
            name="binary",
            field_delimiter="",
            row_after_delimiter="",
            clickhouse_format="Native" if _binary_format(binary_format) == "native" else "RowBinary",
        )
    normalized = value if value in {"ascii_control", "tab_lf"} else "ascii_control"
    if normalized == "tab_lf":
        return BulkWireDelimiterProfile(
            name="tab_lf",
            field_delimiter="\t",
            row_after_delimiter="\n",
            row_between_delimiter="",
            source_row_terminator="\n",
            clickhouse_format="TabSeparatedRaw",
        )
    return BulkWireDelimiterProfile(
        name="ascii_control",
        field_delimiter="\x1f",
        row_after_delimiter="\x1e",
        row_between_delimiter="\n",
        source_row_terminator="\x1e\n",
        clickhouse_format="CustomSeparated",
        clickhouse_settings={
            "format_custom_field_delimiter": "\x1f",
            "format_custom_row_before_delimiter": "",
            "format_custom_row_after_delimiter": "\x1e",
            "format_custom_row_between_delimiter": "\n",
            "format_custom_result_before_delimiter": "",
            "format_custom_result_after_delimiter": "",
            "format_custom_escaping_rule": "CSV",
        },
    )


def _column(name: str, dtype: str) -> BulkWireColumn:
    normalized = str(dtype).strip().lower()
    nullable = "nullable" in normalized or normalized.startswith("null")
    return BulkWireColumn(
        name=str(name),
        source_type=str(dtype),
        target_type=str(dtype),
        nullable=nullable,
        wire_type="raw",
        sidecar_columns=(f"{name}__dpone_null",) if nullable else (),
    )


def _warnings(route: str, policy: BulkWirePolicy, profile: BulkWireDelimiterProfile) -> tuple[str, ...]:
    warnings: list[str] = []
    if route == "typed_binary_bcp_native":
        warnings.append("bulk_wire_bcp_native_source_query_is_not_sink_escaped")
    if route == "typed_binary_bcp_native" and _binary_format(policy.binary_format) == "native":
        warnings.append("bulk_wire_clickhouse_native_columnar_blocks")
    if route == "typed_binary_row_stream":
        warnings.append("bulk_wire_source_query_is_not_sink_escaped")
    if route == "typed_raw_direct":
        warnings.append("bulk_wire_typed_raw_requires_delimiter_safety")
    if policy.text_safety == "source_escape":
        warnings.append("bulk_wire_source_escape_requested")
    if profile.name == "tab_lf":
        warnings.append("bulk_wire_tab_lf_requires_no_tabs_or_lf_in_values")
    return tuple(warnings)


def _schema_hash(schema: Sequence[tuple[str, str]]) -> str:
    payload = repr(tuple((str(name), str(dtype)) for name, dtype in schema)).encode()
    return "sha256:" + sha256(payload).hexdigest()


def _ingest_contract(options: Mapping[str, Any] | None) -> str:
    bulk = _mapping((options or {}).get("clickhouse_bulk"))
    return str(bulk.get("ingest_contract") or "auto").strip().lower()


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _normalize(value: str) -> str:
    return str(value).strip().lower().replace("-", "_")


def _binary_format(value: str) -> str:
    if value == "mssql_native":
        return value
    normalized = str(value or "rowbinary").strip().lower().replace("-", "")
    return "native" if normalized == "native" else "rowbinary"


__all__ = [
    "BULK_WIRE_SCHEMA_VERSION",
    "BulkWireColumn",
    "BulkWireContract",
    "BulkWireDelimiterProfile",
    "BulkWirePlanner",
    "BulkWirePolicy",
    "should_use_source_encoded_tsv",
]
