"""ClickHouse TabSeparated escaping helpers for native file ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from dpone.runtime.support.temporal_fidelity import (
    is_naive_timestamp_type,
    naive_timestamp_scale,
    offset_minutes_column_name,
)
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy


@dataclass(frozen=True)
class ClickHouseTabSeparatedCodec:
    """Render SQL expressions that produce ClickHouse TabSeparated-safe values."""

    codec_id: ClassVar[str] = "dpone.clickhouse.tab-separated"
    codec_version: ClassVar[int] = 1
    marker_prefix: ClassVar[str] = "__dpone__tsv__"
    empty_string_marker: ClassVar[str] = "__dpone__tsv__empty"
    escaped_prefix_marker: ClassVar[str] = "__dpone__tsv__prefix"
    field_terminator: ClassVar[str] = "\t"
    row_terminator: ClassVar[str] = "\n"
    type_policy: MssqlClickHouseTypePolicy = MssqlClickHouseTypePolicy()

    def mssql_select_expression(
        self,
        value_sql: str,
        *,
        text_column: bool = True,
        source_type: str | None = None,
    ) -> str:
        normalized_type = str(source_type or "").strip().lower()
        if self._is_binary(normalized_type) and self.type_policy.binary_encoding == "hex":
            expression = f"CONVERT(NVARCHAR(MAX), CONVERT(VARCHAR(MAX), CONVERT(VARBINARY(MAX), {value_sql}), 2))"
            return f"CASE WHEN {value_sql} IS NULL THEN N'\\N' ELSE {expression} END"
        if self._is_time(normalized_type) and self.type_policy.time_encoding == "seconds_since_midnight":
            expression = f"CONVERT(VARCHAR(MAX), DATEDIFF(SECOND, CAST('00:00:00' AS time), CAST({value_sql} AS time)))"
        elif is_naive_timestamp_type(normalized_type):
            expression = self._mssql_naive_timestamp_expression(value_sql, normalized_type)
        elif self._is_datetimeoffset(normalized_type):
            expression = self._mssql_datetimeoffset_expression(value_sql)
        elif text_column:
            expression = (
                f"CONVERT(VARCHAR(MAX), CONVERT(NVARCHAR(MAX), {value_sql}) COLLATE Latin1_General_100_CI_AS_SC_UTF8)"
            )
        else:
            expression = f"CONVERT(VARCHAR(MAX), {value_sql})"
        expression = (
            f"REPLACE({expression}, "
            f"{self._mssql_literal(self.marker_prefix)}, "
            f"{self._mssql_literal(self.escaped_prefix_marker)})"
        )
        for old, new in (
            ("\\", "\\\\"),
            ("\t", "\\t"),
            ("\n", "\\n"),
            ("\r", "\\r"),
        ):
            expression = f"REPLACE({expression}, {self._mssql_literal(old)}, {self._mssql_literal(new)})"
        if not text_column:
            return f"CASE WHEN {value_sql} IS NULL THEN N'\\N' ELSE {expression} END"
        return (
            f"CASE WHEN {value_sql} IS NULL THEN N'\\N' "
            f"WHEN {value_sql} = N'' THEN {self._mssql_literal(self.empty_string_marker)} "
            f"ELSE {expression} END"
        )

    def mssql_generated_select_expressions(
        self, column: str, value_sql: str, source_type: str | None = None
    ) -> list[tuple[str, str]]:
        """Return generated projection columns required by the active policy."""
        normalized_type = str(source_type or "").strip().lower()
        if not self._is_datetimeoffset(normalized_type):
            return []
        if self.type_policy.temporal.offset_timestamp_mode != "preserve_offset":
            return []
        expression = self.mssql_offset_minutes_expression(value_sql)
        return [(offset_minutes_column_name(column), expression)]

    def clickhouse_staging_type(self, source_type: str, target_type: str) -> str:
        """Return the raw ClickHouse type needed for the encoded TSV value."""

        if self._uses_epoch_naive_timestamp(source_type):
            return self._nullable_type("Int64") if self._is_nullable(source_type, target_type) else "Int64"
        return target_type

    def clickhouse_decode_expression_for_type(
        self,
        value_sql: str,
        *,
        source_type: str,
        target_type: str,
    ) -> str | None:
        """Return a type-aware ClickHouse decode expression for staged TSV values."""

        if not self._uses_epoch_naive_timestamp(source_type):
            return None
        scale = naive_timestamp_scale(source_type)
        factor = 10 ** (9 - scale)
        epoch_value = f"toInt64({value_sql})"
        nanoseconds = epoch_value if factor == 1 else f"({epoch_value} * {factor})"
        cast_type = self._non_nullable_type(target_type)
        decoded = f"CAST(fromUnixTimestamp64Nano({nanoseconds}, 'UTC') AS {cast_type})"
        if self._is_nullable(source_type, target_type):
            return f"if(isNull({value_sql}), NULL, {decoded})"
        return decoded

    def mssql_offset_minutes_expression(self, value_sql: str) -> str:
        expression = f"CONVERT(VARCHAR(MAX), DATEPART(TZOFFSET, CAST({value_sql} AS datetimeoffset)))"
        return f"CASE WHEN {value_sql} IS NULL THEN N'\\N' ELSE {expression} END"

    def _mssql_datetimeoffset_expression(self, value_sql: str) -> str:
        mode = self.type_policy.temporal.offset_timestamp_mode
        if mode == "preserve_text":
            return f"CONVERT(VARCHAR(MAX), CAST({value_sql} AS datetimeoffset), 126)"
        if mode == "fixed_timezone":
            timezone_name = self.type_policy.temporal.sql_server_timezone_name()
            if timezone_name.startswith(("+", "-")):
                return (
                    "CONVERT(VARCHAR(MAX), "
                    f"CAST(SWITCHOFFSET(CAST({value_sql} AS datetimeoffset), '{timezone_name}') AS datetime2(7)), "
                    "121)"
                )
            timezone = self._mssql_literal(timezone_name)
            return (
                "CONVERT(VARCHAR(MAX), "
                f"CAST(CAST({value_sql} AS datetimeoffset) AT TIME ZONE {timezone} AS datetime2(7)), "
                "121)"
            )
        return (
            "CONVERT(VARCHAR(MAX), "
            f"CAST(SWITCHOFFSET(CAST({value_sql} AS datetimeoffset), '+00:00') AS datetime2(7)), "
            "121)"
        )

    def _mssql_naive_timestamp_expression(self, value_sql: str, normalized_type: str) -> str:
        policy = self.type_policy.naive_temporal
        scale = naive_timestamp_scale(normalized_type)
        casted = self._mssql_naive_timestamp_cast(value_sql, scale)
        if policy.effective_transfer_encoding(native_datetime64_path=True) == "epoch":
            factor = 10**scale
            fraction_divisor = 10 ** (9 - scale) if scale else 1
            seconds = f"DATEDIFF_BIG(SECOND, CAST('1970-01-01T00:00:00' AS datetime2({scale})), {casted})"
            if scale == 0:
                ticks = seconds
            else:
                fraction = f"(DATEPART(NANOSECOND, {casted}) / {fraction_divisor})"
                ticks = f"(({seconds}) * {factor} + {fraction})"
            return f"CONVERT(VARCHAR(MAX), {ticks})"
        if normalized_type == "datetime":
            return f"CONVERT(VARCHAR(MAX), {casted}, 121)"
        if normalized_type == "smalldatetime":
            return f"CONVERT(VARCHAR(MAX), {casted}, 120)"
        return f"REPLACE(CONVERT(VARCHAR(MAX), {casted}, 126), 'T', ' ')"

    def _mssql_naive_timestamp_cast(self, value_sql: str, scale: int) -> str:
        policy = self.type_policy.naive_temporal
        casted = f"CAST({value_sql} AS datetime2({scale}))"
        timezone_name = policy.sql_server_timezone_name()
        if timezone_name in {"UTC", "Etc/UTC", "+00:00"}:
            return casted
        if timezone_name.startswith(("+", "-")):
            return f"CAST(SWITCHOFFSET(TODATETIMEOFFSET({casted}, '{timezone_name}'), '+00:00') AS datetime2({scale}))"
        timezone = self._mssql_literal(timezone_name)
        return (
            "CAST(SWITCHOFFSET("
            f"CAST({casted} AT TIME ZONE {timezone} AS datetimeoffset), "
            "'+00:00') AS datetime2("
            f"{scale}))"
        )

    def clickhouse_decode_expression(self, value_sql: str) -> str:
        decoded = f"if({value_sql} = {self._clickhouse_literal(self.empty_string_marker)}, '', {value_sql})"
        return (
            f"replace({decoded}, "
            f"{self._clickhouse_literal(self.escaped_prefix_marker)}, "
            f"{self._clickhouse_literal(self.marker_prefix)})"
        )

    def _uses_epoch_naive_timestamp(self, source_type: str | None) -> bool:
        normalized_type = str(source_type or "").strip().lower()
        if not is_naive_timestamp_type(normalized_type):
            return False
        policy = self.type_policy.naive_temporal
        return policy.effective_transfer_encoding(native_datetime64_path=True) == "epoch"

    @staticmethod
    def _mssql_literal(value: str) -> str:
        parts: list[str] = []
        text_buffer: list[str] = []
        for char in value:
            if ord(char) < 32:
                if text_buffer:
                    parts.append("N'" + "".join(text_buffer).replace("'", "''") + "'")
                    text_buffer = []
                parts.append(f"NCHAR({ord(char)})")
            else:
                text_buffer.append(char)
        if text_buffer:
            parts.append("N'" + "".join(text_buffer).replace("'", "''") + "'")
        return " + ".join(parts) if parts else "N''"

    @staticmethod
    def _clickhouse_literal(value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    @staticmethod
    def _is_nullable(source_type: str | None, target_type: str) -> bool:
        source = str(source_type or "").strip().lower()
        target = str(target_type).strip()
        return " nullable" in source or source.startswith("nullable(") or target.startswith("Nullable(")

    @staticmethod
    def _nullable_type(clickhouse_type: str) -> str:
        return clickhouse_type if clickhouse_type.startswith("Nullable(") else f"Nullable({clickhouse_type})"

    @staticmethod
    def _non_nullable_type(clickhouse_type: str) -> str:
        normalized = str(clickhouse_type).strip()
        if normalized.startswith("Nullable(") and normalized.endswith(")"):
            return normalized[len("Nullable(") : -1]
        return normalized

    @staticmethod
    def _is_binary(normalized_type: str) -> bool:
        return any(token in normalized_type for token in ("binary", "varbinary", "rowversion", "timestamp"))

    @staticmethod
    def _is_time(normalized_type: str) -> bool:
        return normalized_type.startswith("time")

    @staticmethod
    def _is_datetimeoffset(normalized_type: str) -> bool:
        return normalized_type.startswith("datetimeoffset")
