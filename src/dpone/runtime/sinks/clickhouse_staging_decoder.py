"""ClickHouse staging decode helpers for native text artifacts."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from inspect import signature
from typing import TYPE_CHECKING, Any, Protocol

from dpone.runtime.bulk_wire import BULK_WIRE_SCHEMA_VERSION
from dpone.runtime.process_io import add_exception_note
from dpone.runtime.sinks.load_payload import LoadPayload

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class ClickHouseDecodeCodec(Protocol):
    """Codec that can render a ClickHouse-side decode expression."""

    def clickhouse_decode_expression(self, value_sql: str) -> str:
        """Return SQL expression that decodes a staged text value."""


@dataclass(frozen=True, slots=True)
class ClickHouseStagingSchema:
    """Schema contract for the first staging table."""

    columns: Sequence[tuple[str, str]]
    clickhouse_types: bool = False


class ClickHouseStagingDecoder:
    """Build decoded staging tables for lossless native text bulk loads."""

    def __init__(
        self,
        *,
        connector: Any,
        table_name: Callable[[LoadConfig], str],
        create_staging_table: Callable[[LoadConfig, Sequence[tuple[str, str]]], LoadConfig],
        map_type: Callable[..., str],
        drop_staging_table: Callable[[LoadConfig], None] | None = None,
        plan_staging_table: Callable[[LoadConfig], LoadConfig] | None = None,
        create_planned_staging_table: Callable[[LoadConfig, Sequence[tuple[str, str]]], None] | None = None,
    ) -> None:
        if (plan_staging_table is None) != (create_planned_staging_table is None):
            raise ValueError("clickhouse_staging_decoder_plan_callbacks_incomplete")
        self._connector = connector
        self._table_name = table_name
        self._create_staging_table = create_staging_table
        self._map_type = map_type
        self._drop_staging_table = drop_staging_table
        self._plan_staging_table = plan_staging_table
        self._create_planned_staging_table = create_planned_staging_table
        self._map_type_accepts_config = len(signature(map_type).parameters) >= 2

    def staging_schema(self, load_config: LoadConfig, payload: LoadPayload) -> ClickHouseStagingSchema:
        """Return the raw staging schema required before optional decoding."""

        if getattr(payload.artifact, "schema_type_dialect", None) == "clickhouse":
            return ClickHouseStagingSchema(columns=payload.schema, clickhouse_types=True)
        codec = self._text_codec(payload.artifact)
        if codec is None:
            return ClickHouseStagingSchema(columns=payload.schema)
        return ClickHouseStagingSchema(
            columns=[(column, self._staging_type(load_config, dtype, codec)) for column, dtype in payload.schema],
            clickhouse_types=True,
        )

    def bulk_wire_evidence(self, load_config: LoadConfig, payload: LoadPayload) -> dict[str, Any]:
        """Return stable typed-wire evidence for plan/run artifacts."""

        del load_config
        contract = self._bulk_wire_contract(payload.artifact)
        if contract is None:
            return {
                "schema_version": BULK_WIRE_SCHEMA_VERSION,
                "selected_route": "not_configured",
                "mssql_source_escaping": None,
            }
        return {
            "schema_version": BULK_WIRE_SCHEMA_VERSION,
            "selected_route": contract.selected_route,
            "input_format": contract.input_format,
            "schema_hash": contract.schema_hash,
            "mssql_source_escaping": bool(contract.source_escaping),
            "warnings": list(contract.warnings),
            "blockers": list(contract.blockers),
        }

    def prepare(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        payload: LoadPayload,
    ) -> tuple[LoadConfig, LoadConfig | None]:
        """Return finalization staging config and optional decoded staging config."""
        codec = self._text_codec(payload.artifact)
        if codec is None:
            return staging_config, None

        decoded_config: LoadConfig | None = None
        try:
            if self._plan_staging_table is None:
                decoded_config = self._create_staging_table(load_config, payload.schema)
            else:
                decoded_config = self._plan_staging_table(load_config)
                assert self._create_planned_staging_table is not None
                self._create_planned_staging_table(decoded_config, payload.schema)
            columns = [column for column, _ in payload.schema]
            column_sql = ", ".join(f"`{column}`" for column in columns)
            select_sql = ", ".join(
                self._decoded_select_expression(load_config, column, dtype, codec) for column, dtype in payload.schema
            )
            self._connector.execute_query(
                f"INSERT INTO {self._table_name(decoded_config)} ({column_sql}) "
                f"SELECT {select_sql} FROM {self._table_name(staging_config)}"
            )
        except BaseException as error:
            if decoded_config is not None and self._drop_staging_table is not None:
                try:
                    self._drop_staging_table(decoded_config)
                except Exception as cleanup_error:
                    add_exception_note(error, f"decoded staging cleanup failed: {type(cleanup_error).__name__}")
            raise
        return decoded_config, decoded_config

    def _decoded_select_expression(
        self,
        load_config: LoadConfig,
        column: str,
        dtype: str,
        codec: ClickHouseDecodeCodec,
    ) -> str:
        column_sql = f"`{column}`"
        mapped_type = self._mapped_type(dtype, load_config)
        typed_expression = self._typed_decode_expression(column_sql, dtype, mapped_type, codec)
        if typed_expression is not None:
            return f"{typed_expression} AS {column_sql}"
        if mapped_type in {"String", "Nullable(String)"}:
            return f"{codec.clickhouse_decode_expression(column_sql)} AS {column_sql}"
        return column_sql

    def _staging_type(self, load_config: LoadConfig, dtype: str, codec: ClickHouseDecodeCodec) -> str:
        mapped_type = self._mapped_type(dtype, load_config)
        if not hasattr(codec, "clickhouse_staging_type"):
            return mapped_type
        return codec.clickhouse_staging_type(dtype, mapped_type)

    @staticmethod
    def _typed_decode_expression(
        value_sql: str,
        dtype: str,
        mapped_type: str,
        codec: ClickHouseDecodeCodec,
    ) -> str | None:
        if not hasattr(codec, "clickhouse_decode_expression_for_type"):
            return None
        return codec.clickhouse_decode_expression_for_type(
            value_sql,
            source_type=dtype,
            target_type=mapped_type,
        )

    def _mapped_type(self, dtype: str, load_config: LoadConfig) -> str:
        if self._map_type_accepts_config:
            return self._map_type(dtype, load_config)
        return self._map_type(dtype)

    @staticmethod
    def _text_codec(artifact: Any) -> ClickHouseDecodeCodec | None:
        codec = getattr(artifact, "bulk_text_codec", None)
        if codec is not None and hasattr(codec, "clickhouse_decode_expression"):
            return codec
        for partition in getattr(artifact, "partitions", []) or []:
            codec = getattr(partition, "bulk_text_codec", None)
            if codec is not None and hasattr(codec, "clickhouse_decode_expression"):
                return codec
        return None

    @staticmethod
    def _bulk_wire_contract(artifact: Any) -> Any | None:
        contract = getattr(artifact, "bulk_wire_contract", None)
        if contract is not None:
            return contract
        for partition in getattr(artifact, "partitions", []) or []:
            contract = getattr(partition, "bulk_wire_contract", None)
            if contract is not None:
                return contract
        return None
