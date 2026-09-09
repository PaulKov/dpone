"""ClickHouse object-storage pull loader for columnar staging."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class ClickHouseColumnarPullConfig:
    use_cluster_function: str = "auto"
    cluster: str | None = None
    auth_mode: str = "named_collection"
    settings: dict[str, object] | None = None

    @classmethod
    def from_load_config(cls, load_config: LoadConfig) -> ClickHouseColumnarPullConfig:
        options = getattr(load_config, "options", {}) or {}
        bulk = _mapping(options.get("clickhouse_bulk"))
        pull = _mapping(bulk.get("columnar_pull"))
        return cls(
            use_cluster_function=str(pull.get("use_cluster_function") or "auto"),
            cluster=_text(pull.get("cluster")),
            auth_mode=str(pull.get("auth_mode") or "named_collection"),
            settings=_mapping(pull.get("settings")),
        )

    def table_function(self) -> str:
        if self.use_cluster_function == "s3Cluster" or (self.use_cluster_function == "auto" and self.cluster):
            return "s3Cluster"
        return "s3"


class ClickHouseColumnarPullLoader:
    """Stage Parquet objects by asking ClickHouse to pull from object storage."""

    def __init__(
        self,
        *,
        connector: Any,
        table_name: Callable[[LoadConfig], str],
        count_rows: Callable[[LoadConfig], int],
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._connector = connector
        self._table_name = table_name
        self._count_rows = count_rows
        self._clock = clock or perf_counter

    def load(
        self,
        load_config: LoadConfig,
        manifest: Any,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        sql = self.render_insert_sql(load_config, manifest, schema)
        result = self._connector.execute_query(sql)
        return int(result) if isinstance(result, int) else self._count_rows(load_config)

    def load_windowed(
        self,
        load_config: LoadConfig,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        loaded_any = False
        for window_index, window in enumerate(artifact.iter_windows(), start=1):
            sql = self.render_insert_sql(load_config, window, schema)
            pull_started = self._clock()
            self._connector.execute_query(sql)
            pull_seconds = _elapsed(pull_started, self._clock())
            loaded_any = True
            cleanup = getattr(window, "cleanup", None)
            cleanup_seconds = 0.0
            if callable(cleanup):
                cleanup()
                cleanup_seconds = _elapsed(pull_started + pull_seconds, self._clock())
            _record_window_metric(
                artifact,
                window=window,
                window_index=window_index,
                clickhouse_pull_seconds=pull_seconds,
                window_cleanup_seconds=cleanup_seconds,
            )
        return self._count_rows(load_config) if loaded_any else 0

    def render_insert_sql(
        self,
        load_config: LoadConfig,
        manifest: Any,
        schema: Sequence[tuple[str, str]],
    ) -> str:
        config = ClickHouseColumnarPullConfig.from_load_config(load_config)
        columns = [column for column, _ in schema]
        column_sql = ", ".join(_quote_identifier(column) for column in columns)
        return (
            f"INSERT INTO {self._table_name(load_config)} ({column_sql}) "
            f"SELECT {column_sql} FROM {self._table_function_sql(config, manifest)}"
            f"{_settings_clause(config.settings)}"
        )

    def _table_function_sql(
        self,
        config: ClickHouseColumnarPullConfig,
        manifest: Any,
    ) -> str:
        if manifest.read_contract.mode != "named_collection":
            raise ValueError("ClickHouse columnar pull v1 requires named_collection read access")
        collection = _safe_named_collection(manifest.read_contract.named_collection)
        key_pattern = manifest.object_key_pattern().replace("'", "''")
        function = config.table_function()
        if function == "s3Cluster":
            if not config.cluster:
                raise ValueError("clickhouse_bulk.columnar_pull.cluster is required for s3Cluster")
            return f"s3Cluster('{_escape_literal(config.cluster)}', {collection}, filename='{key_pattern}')"
        return f"s3({collection}, filename='{key_pattern}')"


def _quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _safe_named_collection(value: str | None) -> str:
    if not value or not IDENTIFIER_RE.match(value):
        raise ValueError("ClickHouse named_collection must be a safe identifier")
    return value


def _escape_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _settings_clause(settings: dict[str, object] | None) -> str:
    merged = {"input_format_parquet_allow_missing_columns": False, **(settings or {})}
    if not merged:
        return ""
    rendered = ", ".join(f"{key} = {_setting_value(value)}" for key, value in sorted(merged.items()))
    return f" SETTINGS {rendered}"


def _setting_value(value: object) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    return "'" + _escape_literal(str(value)) + "'"


def _mapping(value: object | None) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _record_window_metric(
    artifact: Any,
    *,
    window: Any,
    window_index: int,
    clickhouse_pull_seconds: float,
    window_cleanup_seconds: float,
) -> None:
    recorder = getattr(artifact, "record_window_metric", None)
    if not callable(recorder):
        return
    row_count = int(getattr(window, "row_count", 0) or 0)
    metric = {
        "schema_version": "dpone.native_transfer.columnar_window_metrics.v1",
        "window_index": window_index,
        "row_count": row_count,
        "size_bytes": int(getattr(window, "size_bytes", 0) or 0),
        "uri_prefix": str(getattr(window, "uri_prefix", "")),
        "producer_metrics": dict(getattr(window, "producer_metrics", {}) or {}),
        "clickhouse_pull_seconds": clickhouse_pull_seconds,
        "window_cleanup_seconds": window_cleanup_seconds,
        "rows_per_second": _rows_per_second(row_count, clickhouse_pull_seconds),
    }
    recorder(metric)


def _rows_per_second(rows: int, seconds: float) -> float | None:
    if seconds <= 0:
        return None
    return rows / seconds


def _elapsed(started: float, finished: float) -> float:
    return max(0.0, finished - started)


__all__ = [
    "ClickHouseColumnarPullConfig",
    "ClickHouseColumnarPullLoader",
]
