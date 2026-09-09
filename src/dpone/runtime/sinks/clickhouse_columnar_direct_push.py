"""ClickHouse direct push loader for local columnar chunks."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from dpone.runtime.clickhouse_bulk_path import resolve_clickhouse_bulk_path

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class ClickHouseColumnarDirectPushLoader:
    """Stage local Parquet chunks by pushing them directly to ClickHouse."""

    def __init__(
        self,
        *,
        table_name: Callable[[LoadConfig], str],
        count_rows: Callable[[LoadConfig], int],
        client_runner_factory: Callable[..., Any],
        http_runner_factory: Callable[..., Any],
    ) -> None:
        self._table_name = table_name
        self._count_rows = count_rows
        self._client_runner_factory = client_runner_factory
        self._http_runner_factory = http_runner_factory

    def load(self, load_config: LoadConfig, manifest: Any, schema: Sequence[tuple[str, str]]) -> int:
        columns = [column for column, _ in schema]
        for chunk in manifest.chunks:
            self._insert_chunk(load_config, columns, manifest.format, chunk.path)
        return self._count_rows(load_config) if manifest.chunks else 0

    def load_chunked(self, load_config: LoadConfig, artifact: Any, schema: Sequence[tuple[str, str]]) -> int:
        columns = [column for column, _ in schema]
        loaded_any = False
        for chunk in artifact.iter_chunks():
            self._insert_chunk(load_config, columns, artifact.format, chunk.path)
            loaded_any = True
        return self._count_rows(load_config) if loaded_any else 0

    def _insert_chunk(self, load_config: LoadConfig, columns: Sequence[str], input_format: str, path: Any) -> None:
        self._runner(load_config, input_format).insert_file(
            self._table_name(load_config),
            columns,
            str(path),
        )

    def _runner(self, load_config: LoadConfig, input_format: str) -> Any:
        normalized = _clickhouse_format(input_format)
        path = resolve_clickhouse_bulk_path(getattr(load_config, "options", {}) or {})
        if path == "http":
            return self._http_runner_factory(load_config, input_format=normalized)
        if path == "client":
            return self._client_runner_factory(load_config, input_format=normalized)
        raise RuntimeError("clickhouse_columnar_direct_push_requires_client_or_http")


def _clickhouse_format(value: str) -> str:
    if value.lower() == "parquet":
        return "Parquet"
    return value


__all__ = ["ClickHouseColumnarDirectPushLoader"]
