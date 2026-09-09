"""Canonical bulk-load option normalization.

All flat ``bulk_mode`` and ``bcp_*`` compatibility is intentionally isolated
here. Runtime code should consume ``BulkOptions`` instead of reading ad-hoc
prefixed keys from manifests.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from dpone.runtime.clickhouse_bulk_options_models import (
    ClickHouseBulkOptions,
    ClickHouseClientBulkOptions,
    ClickHouseHttpBulkOptions,
    ClickHouseNativeTcpBulkOptions,
)
from dpone.runtime.connectors.mssql_bulk import (
    DEFAULT_MSSQL_BCP_TIMEOUT_SECONDS,
    BcpOptions,
    effective_mssql_bcp_packet_size,
)


@dataclass(frozen=True, slots=True)
class BcpBulkOptions:
    """Canonical MSSQL bcp settings."""

    bcp_path: str | None = None
    file_format: str = "character"
    code_page: str = "65001"
    field_terminator: str = "\t"
    row_terminator: str = "\n"
    batch_size: int = 100_000
    packet_size: int = 16_384
    timeout_seconds: int | None = DEFAULT_MSSQL_BCP_TIMEOUT_SECONDS
    error_file: str | None = None
    table_lock: bool = True
    keep_nulls: bool = True

    def to_bcp_options(
        self,
        *,
        bcp_path: str,
        trust_server_certificate: bool,
        file_format: str | None = None,
        field_terminator: str | None = None,
        row_terminator: str | None = None,
    ) -> BcpOptions:
        return BcpOptions(
            bcp_path=self.bcp_path or bcp_path,
            file_format=file_format or self.file_format,
            code_page=self.code_page,
            field_terminator=field_terminator or self.field_terminator,
            row_terminator=row_terminator or self.row_terminator,
            batch_size=self.batch_size,
            packet_size=self.packet_size,
            timeout_seconds=self.timeout_seconds,
            error_file=self.error_file,
            trust_server_certificate=trust_server_certificate,
            table_lock=self.table_lock,
            keep_nulls=self.keep_nulls,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BulkOptions:
    """Canonical bulk-load options."""

    mode: str
    bcp: BcpBulkOptions
    deprecated_aliases: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "bcp": self.bcp.to_dict(),
            "deprecated_aliases": list(self.deprecated_aliases),
        }


class ClickHouseBulkOptionsResolver:
    """Normalize ClickHouse direct-ingest options."""

    @classmethod
    def resolve(cls, options: Mapping[str, Any] | None, *, default_mode: str = "auto") -> ClickHouseBulkOptions:
        raw = dict(options or {})
        canonical = raw.get("clickhouse_bulk") if isinstance(raw.get("clickhouse_bulk"), Mapping) else {}
        canonical = dict(canonical or {})
        raw_client = canonical.get("client")
        raw_http = canonical.get("http")
        raw_native_tcp = canonical.get("native_tcp")
        raw_streaming = canonical.get("streaming")
        client: Mapping[str, Any] = raw_client if isinstance(raw_client, Mapping) else {}
        http: Mapping[str, Any] = raw_http if isinstance(raw_http, Mapping) else {}
        native_tcp: Mapping[str, Any] = raw_native_tcp if isinstance(raw_native_tcp, Mapping) else {}
        streaming: Mapping[str, Any] = raw_streaming if isinstance(raw_streaming, Mapping) else {}
        deprecated: list[str] = []
        profile = _optimizer_profile(raw)

        mode = canonical.get("mode")
        if mode is None and raw.get("clickhouse_bulk_mode") is not None:
            mode = raw.get("clickhouse_bulk_mode")
            deprecated.append("sink.options.clickhouse_bulk_mode")

        explicit_insert_settings = dict(canonical.get("insert_settings") or {})
        insert_settings = dict(_clickhouse_profile_insert_settings(profile))
        insert_settings.update(explicit_insert_settings)
        if streaming.get("async_insert") is not None and "async_insert" not in explicit_insert_settings:
            insert_settings["async_insert"] = 1 if _optional_bool(streaming.get("async_insert"), default=False) else 0
        if (
            streaming.get("wait_for_async_insert") is not None
            and "wait_for_async_insert" not in explicit_insert_settings
        ):
            insert_settings["wait_for_async_insert"] = (
                1 if _optional_bool(streaming.get("wait_for_async_insert"), default=False) else 0
            )
        if not insert_settings and raw.get("clickhouse_insert_settings") is not None:
            explicit_insert_settings = dict(raw.get("clickhouse_insert_settings") or {})
            insert_settings = dict(_clickhouse_profile_insert_settings(profile))
            insert_settings.update(explicit_insert_settings)
            deprecated.append("sink.options.clickhouse_insert_settings")
        async_insert = insert_settings.get("async_insert")
        if (
            async_insert in {0, "0", False, "false", "False"}
            and "wait_for_async_insert" not in explicit_insert_settings
        ):
            insert_settings.pop("wait_for_async_insert", None)
        if async_insert in {1, "1", True, "true", "True"} and "wait_for_async_insert" not in insert_settings:
            insert_settings["wait_for_async_insert"] = 1

        client_options = ClickHouseClientBulkOptions(
            command=_ch_value(
                client, raw, deprecated, "command", "clickhouse_client_command", "clickhouse_client_path"
            ),
            host=_ch_value(client, raw, deprecated, "host", "clickhouse_client_host"),
            port=_optional_int(_ch_value(client, raw, deprecated, "port", "clickhouse_client_port")),
            database=_ch_value(client, raw, deprecated, "database", "clickhouse_client_database"),
            user=_ch_value(client, raw, deprecated, "user", "clickhouse_client_user"),
            password=_ch_value(client, raw, deprecated, "password", "clickhouse_client_password"),
            secure=_optional_bool(
                _ch_value(client, raw, deprecated, "secure", "clickhouse_client_secure"), default=None
            ),
            timeout_seconds=_optional_int(
                _ch_value(client, raw, deprecated, "timeout_seconds", "clickhouse_client_timeout_seconds")
            ),
        )
        http_options = ClickHouseHttpBulkOptions(
            host=_ch_value(http, raw, deprecated, "host", "clickhouse_http_host"),
            port=_optional_int(_ch_value(http, raw, deprecated, "port", "clickhouse_http_port")),
            database=_ch_value(http, raw, deprecated, "database", "clickhouse_http_database"),
            user=_ch_value(http, raw, deprecated, "user", "clickhouse_http_user"),
            password=_ch_value(http, raw, deprecated, "password", "clickhouse_http_password"),
            secure=bool(
                _optional_bool(_ch_value(http, raw, deprecated, "secure", "clickhouse_http_secure"), default=False)
            ),
            timeout_seconds=int(
                _ch_value(http, raw, deprecated, "timeout_seconds", "clickhouse_http_timeout_seconds") or 3600
            ),
            chunk_size=int(
                _ch_value(http, raw, deprecated, "chunk_size", "clickhouse_http_chunk_size")
                or _clickhouse_profile_http_chunk_size(profile)
                or 1024 * 1024
            ),
        )
        native_tcp_options = ClickHouseNativeTcpBulkOptions(
            enabled=bool(_optional_bool(native_tcp.get("enabled"), default=False)),
            backend=_native_tcp_backend(native_tcp.get("backend")),
            compression=str(native_tcp.get("compression") or "auto").strip().lower(),
            host=str(native_tcp["host"]) if native_tcp.get("host") is not None else None,
            port=int(native_tcp.get("port") or 9000),
            secure=bool(_optional_bool(native_tcp.get("secure"), default=False)),
            timeout_seconds=_optional_int(native_tcp.get("timeout_seconds")),
            connection_pool_size=max(1, int(native_tcp.get("connection_pool_size") or 2)),
            query_timeout_seconds=int(native_tcp.get("query_timeout_seconds") or 3600),
        )
        warnings = tuple(
            f"{alias} is deprecated; use sink.options.clickhouse_bulk.{_clickhouse_canonical_name(alias)}."
            for alias in deprecated
        )
        return ClickHouseBulkOptions(
            mode=str(mode or default_mode),
            client=client_options,
            http=http_options,
            native_tcp=native_tcp_options,
            insert_settings=insert_settings,
            query_id=canonical.get("query_id") or raw.get("clickhouse_query_id"),
            insert_deduplication_token=canonical.get("insert_deduplication_token")
            or raw.get("clickhouse_insert_deduplication_token"),
            deprecated_aliases=tuple(deprecated),
            warnings=warnings,
        )


def _ch_value(
    canonical: Mapping[str, Any], raw: Mapping[str, Any], deprecated: list[str], key: str, *aliases: str
) -> Any:
    if canonical.get(key) is not None:
        return canonical[key]
    for alias in aliases:
        if raw.get(alias) is not None:
            deprecated.append(f"sink.options.{alias}")
            return raw[alias]
    return None


def _clickhouse_canonical_name(alias: str) -> str:
    return {
        "sink.options.clickhouse_bulk_mode": "mode",
        "sink.options.clickhouse_insert_settings": "insert_settings",
        "sink.options.clickhouse_http_host": "http.host",
        "sink.options.clickhouse_http_port": "http.port",
        "sink.options.clickhouse_http_database": "http.database",
        "sink.options.clickhouse_http_user": "http.user",
        "sink.options.clickhouse_http_password": "http.password",
        "sink.options.clickhouse_http_secure": "http.secure",
        "sink.options.clickhouse_http_timeout_seconds": "http.timeout_seconds",
        "sink.options.clickhouse_http_chunk_size": "http.chunk_size",
        "sink.options.clickhouse_client_command": "client.command",
        "sink.options.clickhouse_client_path": "client.command",
        "sink.options.clickhouse_client_host": "client.host",
        "sink.options.clickhouse_client_port": "client.port",
        "sink.options.clickhouse_client_database": "client.database",
        "sink.options.clickhouse_client_user": "client.user",
        "sink.options.clickhouse_client_password": "client.password",
        "sink.options.clickhouse_client_secure": "client.secure",
        "sink.options.clickhouse_client_timeout_seconds": "client.timeout_seconds",
    }.get(alias, alias.removeprefix("sink.options."))


def _native_tcp_backend(value: Any) -> str:
    normalized = str(value or "auto").strip().lower().replace("-", "_")
    return normalized if normalized in {"auto", "direct", "client"} else "auto"


class BulkOptionsResolver:
    """Normalize bulk options and isolate flat legacy aliases."""

    @classmethod
    def resolve(
        cls,
        options: Mapping[str, Any] | None,
        *,
        default_mode: str = "bcp",
        default_batch_size: int = 100_000,
        default_packet_size: int = 16_384,
    ) -> BulkOptions:
        raw = dict(options or {})
        bulk = raw.get("bulk") if isinstance(raw.get("bulk"), Mapping) else {}
        bulk = dict(bulk or {})
        bcp = bulk.get("bcp") if isinstance(bulk.get("bcp"), Mapping) else {}
        bcp = dict(bcp or {})
        deprecated: list[str] = []
        profile = _optimizer_profile(raw)

        mode = bulk.get("mode")
        if mode is None and raw.get("bulk_mode") is not None:
            mode = raw.get("bulk_mode")
            deprecated.append("sink.options.bulk_mode")

        settings = BcpBulkOptions(
            bcp_path=_value(bcp, raw, deprecated, "bcp_path", "bcp_path"),
            file_format=str(_value(bcp, raw, deprecated, "file_format", "mssql_bcp_file_format") or "character"),
            code_page=str(_value(bcp, raw, deprecated, "code_page", "bcp_code_page") or "65001"),
            field_terminator=str(_value(bcp, raw, deprecated, "field_terminator", "field_terminator") or "\t"),
            row_terminator=str(_value(bcp, raw, deprecated, "row_terminator", "row_terminator") or "\n"),
            batch_size=int(
                _value(bcp, raw, deprecated, "batch_size", "bcp_batch_size")
                or _bcp_profile_default(profile, "batch_size")
                or default_batch_size
            ),
            packet_size=effective_mssql_bcp_packet_size(
                int(
                    _value(bcp, raw, deprecated, "packet_size", "bcp_packet_size")
                    or _bcp_profile_default(profile, "packet_size")
                    or default_packet_size
                )
            ),
            timeout_seconds=_bcp_timeout_seconds(
                _value(bcp, raw, deprecated, "timeout_seconds", "bcp_timeout_seconds"),
                profile=profile,
            ),
            error_file=_value(bcp, raw, deprecated, "error_file", "bcp_error_file"),
            table_lock=bool(_optional_bool(_value(bcp, raw, deprecated, "table_lock", "bcp_table_lock"), default=True)),
            keep_nulls=bool(_optional_bool(_value(bcp, raw, deprecated, "keep_nulls", "bcp_keep_nulls"), default=True)),
        )
        warnings = tuple(
            f"{alias} is deprecated; use sink.options.bulk.{_canonical_name(alias)}." for alias in deprecated
        )
        return BulkOptions(
            mode=str(mode or default_mode),
            bcp=settings,
            deprecated_aliases=tuple(deprecated),
            warnings=warnings,
        )


def _value(canonical: Mapping[str, Any], raw: Mapping[str, Any], deprecated: list[str], key: str, alias: str) -> Any:
    if canonical.get(key) is not None:
        return canonical[key]
    if raw.get(alias) is not None:
        deprecated.append(f"sink.options.{alias}")
        return raw[alias]
    return None


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _bcp_timeout_seconds(value: Any, *, profile: str) -> int:
    """Resolve one explicit positive process deadline without truthy fallback."""

    resolved = value
    if resolved is None:
        resolved = _bcp_profile_default(profile, "timeout_seconds")
    if resolved is None:
        return DEFAULT_MSSQL_BCP_TIMEOUT_SECONDS
    if isinstance(resolved, bool):
        raise ValueError("sink.options.bulk.bcp.timeout_seconds must be a positive integer")
    try:
        timeout = int(resolved)
    except (TypeError, ValueError) as exc:
        raise ValueError("sink.options.bulk.bcp.timeout_seconds must be a positive integer") from exc
    if timeout < 1:
        raise ValueError("sink.options.bulk.bcp.timeout_seconds must be a positive integer")
    return timeout


def _optional_bool(value: Any, *, default: bool | None) -> bool | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _optimizer_profile(raw: Mapping[str, Any]) -> str:
    native_transfer = raw.get("native_transfer")
    if not isinstance(native_transfer, Mapping):
        return ""
    return str(native_transfer.get("optimizer_profile") or "").strip().lower()


def _bcp_profile_default(profile: str, key: str) -> Any:
    if profile != "high_throughput_safe":
        return None
    return {
        "batch_size": 250_000,
        "packet_size": 65_535,
        "timeout_seconds": 3600,
    }.get(key)


def _clickhouse_profile_insert_settings(profile: str) -> dict[str, Any]:
    if profile != "high_throughput_safe":
        return {}
    return {
        "async_insert": 1,
        "wait_for_async_insert": 1,
        "input_format_parallel_parsing": 1,
        "max_insert_block_size": 1_000_000,
    }


def _clickhouse_profile_http_chunk_size(profile: str) -> int | None:
    if profile != "high_throughput_safe":
        return None
    return 4 * 1024 * 1024


def _canonical_name(alias: str) -> str:
    return {
        "sink.options.bulk_mode": "mode",
        "sink.options.bcp_batch_size": "bcp.batch_size",
        "sink.options.bcp_packet_size": "bcp.packet_size",
        "sink.options.bcp_timeout_seconds": "bcp.timeout_seconds",
        "sink.options.bcp_error_file": "bcp.error_file",
        "sink.options.bcp_table_lock": "bcp.table_lock",
        "sink.options.bcp_keep_nulls": "bcp.keep_nulls",
        "sink.options.bcp_code_page": "bcp.code_page",
        "sink.options.mssql_bcp_file_format": "bcp.file_format",
        "sink.options.field_terminator": "bcp.field_terminator",
        "sink.options.row_terminator": "bcp.row_terminator",
        "sink.options.bcp_path": "bcp.bcp_path",
    }.get(alias, alias.removeprefix("sink.options."))


__all__ = [
    "BcpBulkOptions",
    "BulkOptions",
    "BulkOptionsResolver",
    "ClickHouseBulkOptions",
    "ClickHouseBulkOptionsResolver",
]
