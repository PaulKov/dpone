from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy
from dpone.runtime.streaming_rows import StreamingRowsArtifact
from dpone.runtime.support.data_type_mapper import DataTypeMapper


class MindboxBaseStrategy(APIBaseStrategy):
    """Shared extraction helpers for Mindbox resources."""

    @staticmethod
    def _parse_datetime_option(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                return value.astimezone(UTC).replace(tzinfo=None)
            return value.replace(tzinfo=None)
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is not None:
            return parsed.astimezone(UTC).replace(tzinfo=None)
        return parsed.replace(tzinfo=None)

    @staticmethod
    def _mindbox_today(utc_boundary_time: str = "21:00:00") -> date:
        now_utc = datetime.now(UTC)
        try:
            boundary_hour = int(str(utc_boundary_time).split(":", 1)[0])
        except Exception:
            boundary_hour = 21
        if now_utc.hour >= boundary_hour:
            return now_utc.date() + timedelta(days=1)
        return now_utc.date()

    @staticmethod
    def _log_period(
        since: date | datetime | None,
        till: date | datetime | None,
        utc_boundary_time: str,
        *,
        window_mode: str = "datetime_utc",
    ) -> dict[str, str]:
        if since is None and till is None:
            return {"Period": "все данные (без фильтра дат)"}
        if str(window_mode).strip().lower() == "date_project":
            project_result: dict[str, str] = {}
            if since is not None:
                project_result["Project_since_date"] = (
                    since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
                )
            if till is not None:
                project_result["Project_till_date"] = (
                    till.strftime("%Y-%m-%d") if isinstance(till, datetime) else str(till)
                )
            return project_result
        if isinstance(since, datetime) or isinstance(till, datetime):
            datetime_result: dict[str, str] = {}
            if since is not None:
                datetime_result["API_since_utc"] = (
                    since.strftime("%Y-%m-%d %H:%M") if isinstance(since, datetime) else f"{since} {utc_boundary_time}"
                )
            if till is not None:
                datetime_result["API_till_utc"] = (
                    till.strftime("%Y-%m-%d %H:%M") if isinstance(till, datetime) else f"{till} {utc_boundary_time}"
                )
            return datetime_result
        is_msk_boundary = str(utc_boundary_time).startswith("21")
        offset = timedelta(days=1) if is_msk_boundary else timedelta()
        result: dict[str, str] = {}
        if since is not None and till is not None:
            msk_from = since + offset
            msk_to = till
            days = (msk_to - msk_from).days + 1
            result["Period_MSK"] = f"{msk_from} .. {msk_to} ({days} дн.)"
        elif since is not None:
            result["Period_MSK"] = f"с {since + offset}"
        else:
            result["Period_MSK"] = f"до {till}"
        if since is not None:
            result["API_since_utc"] = f"{since} {utc_boundary_time}"
        if till is not None:
            result["API_till_utc"] = f"{till} {utc_boundary_time}"
        return result

    def _parse_common_options(self, load_config: Any) -> tuple[dict[str, Any], dict[str, Any]]:
        options = self._get_options(load_config)
        parsed = {
            "resource": options.get("resource") or getattr(load_config, "source_table", None),
            "batch_size": int(options.get("batch_size", 10_000)),
            "poll_interval": int(options.get("poll_interval", 30)),
            "export_timeout": int(options.get("export_timeout", 3600)),
            "utc_boundary_time": str(options.get("utc_boundary_time", "21:00:00")),
            "csv_delimiter": str(options.get("csv_delimiter", ";")),
            "since_datetime_utc": self._parse_datetime_option(options.get("since_datetime_utc")),
            "till_datetime_utc": self._parse_datetime_option(options.get("till_datetime_utc")),
        }
        return parsed, options

    @staticmethod
    def _build_filters(
        poll_interval: int,
        export_timeout: int,
        utc_boundary_time: str,
        *,
        since: date | datetime | None = None,
        till: date | datetime | None = None,
        csv_delimiter: str = ";",
    ) -> dict[str, Any]:
        filters: dict[str, Any] = {
            "poll_interval": poll_interval,
            "export_timeout": export_timeout,
            "utc_boundary_time": utc_boundary_time,
            "csv_delimiter": csv_delimiter,
        }
        if since is not None:
            filters["since"] = since
        if till is not None:
            filters["till"] = till
        return filters

    @staticmethod
    def _format_sql_window_value(value: date | datetime, utc_boundary_time: str) -> str:
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return f"{value.strftime('%Y-%m-%d')} {utc_boundary_time}"

    def _fetch_and_build_result(
        self,
        resource: str,
        filters: dict[str, Any],
        batch_size: int,
        *,
        force_full_refresh: bool = False,
    ) -> ExtractResult:
        rows_iter = self.connector.get_resources(resource, filters=filters)
        scanned_records: list[dict[str, Any]] = []
        all_keys: list[str] = []
        seen_keys: set[str] = set()
        column_first_values: dict[str, Any] = {}

        for record in rows_iter:
            scanned_records.append(record)
            for key in record:
                if key not in seen_keys:
                    all_keys.append(key)
                    seen_keys.add(key)
            for key, value in record.items():
                if key not in column_first_values and value is not None:
                    column_first_values[key] = value
            if len(column_first_values) == len(all_keys):
                break
            if len(scanned_records) >= 10_000:
                self.logger.warning(
                    "Mindbox %s: schema scan safety limit reached (typed=%s/%s)",
                    resource,
                    len(column_first_values),
                    len(all_keys),
                )
                break

        if not scanned_records:
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=None,
                force_full_refresh=force_full_refresh,
            )

        schema = self._detect_schema_from_first_values(all_keys, column_first_values)
        typed_columns = {name: col_type for name, col_type in schema}

        def _pad_and_coerce(record: dict[str, Any]) -> dict[str, Any]:
            padded = {key: record.get(key) for key in all_keys}
            for key, col_type in typed_columns.items():
                padded[key] = self._coerce_value_for_type(padded.get(key), col_type)
            return padded

        def full_iterator() -> Iterator[dict[str, Any]]:
            for record in scanned_records:
                yield _pad_and_coerce(record)
            for record in rows_iter:
                yield _pad_and_coerce(record)

        artifact = StreamingRowsArtifact(iterator=full_iterator(), batch_size=batch_size)
        return ExtractResult(
            artifact=artifact,
            schema=schema,
            state=None,
            force_full_refresh=force_full_refresh,
        )

    def _detect_schema_from_first_values(
        self,
        all_keys: list[str],
        column_first_values: dict[str, Any],
    ) -> list[tuple[str, str]]:
        target_db = self._get_target_db()
        schema: list[tuple[str, str]] = []
        for column in all_keys:
            value = column_first_values.get(column)
            if value is None:
                db_type = "STRING" if target_db == "bigquery" else "text"
            elif target_db == "bigquery":
                db_type = DataTypeMapper.python_to_bigquery(value)
            else:
                db_type = DataTypeMapper.python_to_postgres(value)
            schema.append((column, db_type))
        return schema

    @staticmethod
    def _coerce_value_for_type(value: Any, db_type: str) -> Any:
        if value is None:
            return None
        normalized = db_type.upper()
        if normalized in {"TIMESTAMP", "DATETIME"} or db_type.lower().startswith("timestamp"):
            parsed = DataTypeMapper.parse_timestamp(value)
            return parsed if parsed is not None else value
        if normalized in {"INT64", "INTEGER"} or db_type.lower() in {"integer", "int", "bigint", "smallint"}:
            try:
                return int(value)
            except Exception:
                return value
        if normalized in {"FLOAT64", "NUMERIC", "BIGNUMERIC"} or db_type.lower() in {
            "double precision",
            "numeric",
            "real",
            "float",
        }:
            try:
                return float(value)
            except Exception:
                return value
        if normalized in {"BOOL", "BOOLEAN"} or db_type.lower() == "boolean":
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in {"true", "1", "yes", "y"}:
                return True
            if text in {"false", "0", "no", "n"}:
                return False
        return value
