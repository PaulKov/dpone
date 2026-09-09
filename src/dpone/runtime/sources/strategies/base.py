"""Базовые стратегии извлечения для источников."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping, Sequence
from typing import (
    Any,
)

from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.streaming_row_guards import assert_dict_rows_preserve_columns
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class SourceStrategy(ABC):
    """Контракт стратегии извлечения."""

    SMALL_RESULT_THRESHOLD = 10_000
    STREAM_BATCH_SIZE = 20_000
    # Generic SQL Server atomicity is an explicit source capability. Unknown
    # third-party strategies must never inherit a safe checkpoint claim.
    mssql_transaction_checkpoint_mode = "unknown"

    @abstractmethod
    def get_state(self, load_config: Any) -> Any | None:
        """Возвращает текущее состояние (если требуется)."""

    @abstractmethod
    def extract(self, load_config: Any, last_state: Any | None) -> ExtractResult:
        """Возвращает данные и новое состояние."""

    def save_state(self, load_config: Any, state: Any) -> None:
        """Опциональное сохранение состояния."""
        return None

    def _build_rows_artifact(
        self,
        connector,
        query,
        schema: Sequence[tuple[str, str]],
        *,
        params: Sequence[Any] | None = None,
        batch_size: int | None = None,
        preflight_count: bool = True,
    ):
        query_sql = self._render_query(connector, query)
        expected_columns = tuple(name for name, _dtype in schema)

        estimated = self._estimate_row_count(connector, query_sql, params) if preflight_count else None
        batch_size = batch_size or self.STREAM_BATCH_SIZE

        if estimated is not None and estimated <= self.SMALL_RESULT_THRESHOLD:
            rows = connector.get_records(query_sql, params, as_dict=True)
            assert_dict_rows_preserve_columns(rows, expected_columns=expected_columns)
            return InMemoryRowsArtifact(rows)

        iterator = self._iter_guarded_dict_rows(
            connector,
            query_sql,
            params=params,
            batch_size=batch_size,
            expected_columns=expected_columns,
        )
        return StreamingRowsArtifact(iterator, batch_size=batch_size, estimated_rows=estimated)

    def _estimate_row_count(self, connector, query: str, params: Sequence[Any] | None = None) -> int | None:
        try:
            count_query = f"SELECT COUNT(*) FROM ({query}) AS estimated_count"
            result = connector.get_records(count_query, params)
            return result[0][0] if result else 0
        except Exception:
            return None

    def _iter_guarded_dict_rows(
        self,
        connector,
        query: Any,
        *,
        params: Sequence[Any] | None = None,
        batch_size: int,
        expected_columns: Sequence[str] = (),
    ) -> Iterator[Mapping[str, Any]]:
        """Stream dict rows and fail closed if business columns disappear."""

        def generator() -> Iterator[Mapping[str, Any]]:
            validated = False
            for batch in connector.get_records_streaming(
                query,
                params,
                batch_size=batch_size,
                as_dict=True,
            ):
                if not validated and batch:
                    assert_dict_rows_preserve_columns(batch, expected_columns=expected_columns)
                    validated = True
                yield from batch

        return generator()

    def _render_query(self, connector, query) -> str:
        if hasattr(query, "as_string"):
            return query.as_string(connector.connection)
        return str(query)
