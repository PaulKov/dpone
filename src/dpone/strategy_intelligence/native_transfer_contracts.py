"""Explainable native transfer transport contracts.

The module is intentionally pure and connector-free. It turns an already
selected source -> sink native path into user-facing safety diagnostics that
``dpone plan`` and certification artifacts can render.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class NativeTransferTransportContract:
    """Losslessness and wire-format contract for one native transfer path."""

    route: str
    wire_format: str
    source_encoding: str
    ingest_mode: str
    null_policy: str
    empty_string_policy: str
    text_codec: str | None
    compression: str
    lossless: bool
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class NativeTransferTransportContractBuilder:
    """Build route-specific transport diagnostics without doing runtime IO."""

    def build(
        self,
        *,
        source_type: str,
        sink_type: str,
        source_options: Mapping[str, Any],
        native_ingest_settings: Mapping[str, Any],
    ) -> NativeTransferTransportContract | None:
        source = _normalize(source_type)
        sink = _normalize(sink_type)
        if source == "postgres" and sink == "mssql":
            return self._postgres_to_mssql(source_options, native_ingest_settings)
        if source == "mysql" and sink == "mssql":
            return self._mysql_to_mssql(source_options, native_ingest_settings)
        return None

    def _postgres_to_mssql(
        self,
        source_options: Mapping[str, Any],
        native_ingest_settings: Mapping[str, Any],
    ) -> NativeTransferTransportContract:
        export_format = _normalize_format(str(source_options.get("export_format") or ""))
        effective_wire_format = export_format or "mssql-delimited"
        compression = "gzip" if bool(source_options.get("compress_export", False)) else "none"
        bulk = native_ingest_settings.get("bulk") if isinstance(native_ingest_settings.get("bulk"), Mapping) else {}
        bulk_mode = str((bulk or {}).get("mode") or "")
        warnings = _mssql_bcp_wire_warnings(
            route="Postgres -> MSSQL",
            export_format=export_format,
            compression=compression,
            bulk_mode=bulk_mode,
        )
        lossless = not warnings
        text_codec = "BulkTextCodec" if effective_wire_format == "mssql-delimited" else None
        return NativeTransferTransportContract(
            route="postgres_to_mssql",
            wire_format=effective_wire_format,
            source_encoding="postgres_copy_to_stdout",
            ingest_mode=bulk_mode or "bcp",
            null_policy="empty_bcp_field_is_null",
            empty_string_policy="encoded_marker_roundtrip" if text_codec else "not_guaranteed",
            text_codec=text_codec,
            compression=compression,
            lossless=lossless,
            warnings=warnings,
        )

    def _mysql_to_mssql(
        self,
        source_options: Mapping[str, Any],
        native_ingest_settings: Mapping[str, Any],
    ) -> NativeTransferTransportContract:
        export_format = _normalize_format(str(source_options.get("export_format") or ""))
        effective_wire_format = export_format or "mssql-delimited"
        compression = "gzip" if bool(source_options.get("compress_export", False)) else "none"
        bulk = native_ingest_settings.get("bulk") if isinstance(native_ingest_settings.get("bulk"), Mapping) else {}
        bulk_mode = str((bulk or {}).get("mode") or "")
        warnings = _mssql_bcp_wire_warnings(
            route="MySQL -> MSSQL",
            export_format=export_format,
            compression=compression,
            bulk_mode=bulk_mode,
        )
        lossless = not warnings
        text_codec = "BulkTextCodec" if effective_wire_format == "mssql-delimited" else None
        return NativeTransferTransportContract(
            route="mysql_to_mssql",
            wire_format=effective_wire_format,
            source_encoding="mysql_streaming_select",
            ingest_mode=bulk_mode or "bcp",
            null_policy="empty_bcp_field_is_null",
            empty_string_policy="encoded_marker_roundtrip" if text_codec else "not_guaranteed",
            text_codec=text_codec,
            compression=compression,
            lossless=lossless,
            warnings=warnings,
        )


def _mssql_bcp_wire_warnings(
    *,
    route: str,
    export_format: str,
    compression: str,
    bulk_mode: str,
) -> tuple[str, ...]:
    warnings: list[str] = []
    if export_format not in {"mssql-delimited"}:
        warnings.append(f"{route} native fast path expects source.options.export_format=mssql-delimited.")
    if compression != "none":
        warnings.append(f"{route} bcp native fast path cannot load gzip artifacts directly.")
    if bulk_mode and bulk_mode != "bcp":
        warnings.append(f"{route} native fast path expects sink.options.bulk.mode=bcp.")
    return tuple(warnings)


def _normalize(value: str) -> str:
    return str(value).strip().lower().replace("-", "_").replace("sqlserver", "mssql")


def _normalize_format(value: str) -> str:
    return value.strip().lower().replace("_", "-")


__all__ = ["NativeTransferTransportContract", "NativeTransferTransportContractBuilder"]
