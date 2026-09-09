from __future__ import annotations

import itertools
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from dpone.runtime.connectors.api.fasttrack_resources import FasttrackResourceSpec, get_fasttrack_resource
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy
from dpone.runtime.streaming_rows import StreamingRowsArtifact
from dpone.runtime.support.fasttrack_columns import normalize_fasttrack_record_columns
from dpone.runtime.support.fasttrack_dates import parse_fasttrack_record_dates, resolve_fasttrack_parse_temporal_fields


class FasttrackBaseExtractStrategy(APIBaseStrategy):
    """Shared helpers for Fasttrack landing ingestion.

    Landing policy is ELT-oriented: we only parse datetime columns for better
    typing/performance, while the rest of the payload stays provider-shaped.
    """

    DEFAULT_BATCH_SIZE = 5_000
    SCHEMA_PREVIEW_ROWS = 100

    def get_state(self, load_config: Any) -> None:
        # FT-2 keeps Fasttrack incremental loads stateless. The API endpoints in
        # the legacy contour were reloaded fully and deduped/merged by keys.
        return None

    def _resolve_resource_spec(self, load_config: Any) -> FasttrackResourceSpec:
        options = self._get_options(load_config)
        resource = options.get("resource") or getattr(load_config, "source_table", None)
        if not resource:
            raise ValueError("Для Fasttrack source.options.resource обязателен")
        return get_fasttrack_resource(str(resource))

    def _resolve_batch_size(self, load_config: Any) -> int:
        options = self._get_options(load_config)
        return int(options.get("batch_size", self.DEFAULT_BATCH_SIZE))

    def _resolve_extra_params(self, load_config: Any, spec: FasttrackResourceSpec) -> dict[str, Any]:
        options = dict(self._get_options(load_config) or {})
        ignore = {
            "resource",
            "batch_size",
            "unique_key",
            "incremental_column",
            "lookback_days",
            "full_refresh_days",
            "log_sample_rows",
            "table_labels",
            "table_description",
            "technical_columns",
            "include_technical_columns",
            "apply_table_metadata",
            "connection_id",
            "connection_type",
            "vault_path",
            "parse_temporal_fields",
            "timeout",
            "max_retries",
            "retries",
            "retry_delay",
            "rate_limit_delay",
        }
        params = dict(spec.default_params)
        params.update({key: value for key, value in options.items() if key not in ignore})
        return params

    def _should_parse_temporal_fields(self, load_config: Any) -> bool:
        return resolve_fasttrack_parse_temporal_fields(self._get_options(load_config))

    def _iter_normalized_rows(self, load_config: Any, spec: FasttrackResourceSpec) -> Iterator[dict[str, Any]]:
        params = self._resolve_extra_params(load_config, spec)
        parse_temporal = self._should_parse_temporal_fields(load_config)
        for row in self.connector.iter_resource_rows(resource_name=spec.name, extra_params=params):
            normalized = normalize_fasttrack_record_columns(spec.name, row)
            if parse_temporal and spec.date_fields:
                normalized = parse_fasttrack_record_dates(normalized, spec.date_fields)
            yield normalized

    def _log_extract_plan(
        self,
        *,
        event: str,
        load_config: Any,
        spec: FasttrackResourceSpec,
        params: Mapping[str, Any],
    ) -> None:
        payload = {
            "API": "fasttrack",
            "Resource": spec.name,
            "Transport": spec.transport,
            "Target": f"{getattr(load_config, 'target_schema', '?')}.{getattr(load_config, 'target_table', '?')}",
            "Batch_Size": self._resolve_batch_size(load_config),
            "Parse_Temporal_Fields": self._should_parse_temporal_fields(load_config),
            "Date_Columns": list(spec.date_fields.keys()),
            "Params": dict(params),
        }
        if hasattr(self.logger, "log_etl_progress"):
            self.logger.log_etl_progress(event, payload)
        else:
            self.logger.info("%s: %s", event, payload)

    def _detect_fasttrack_schema(
        self,
        *,
        spec: FasttrackResourceSpec,
        preview_rows: Sequence[dict[str, Any]],
        parse_temporal: bool,
    ) -> Sequence[tuple[str, str]]:
        from dpone.runtime.support.data_type_mapper import DataTypeMapper

        if not preview_rows:
            return []

        target_db = self._get_target_db()
        ordered_keys: list[str] = []
        seen_keys: set[str] = set()
        first_non_null_values: dict[str, Any] = {}

        for row in preview_rows:
            for key, value in row.items():
                if key not in seen_keys:
                    ordered_keys.append(key)
                    seen_keys.add(key)
                if key not in first_non_null_values and value is not None:
                    first_non_null_values[key] = value

        temporal_columns = set(spec.date_fields) if parse_temporal else set()
        timestamp_type = "TIMESTAMP" if target_db == "bigquery" else "timestamp"

        schema: list[tuple[str, str]] = []
        for key in ordered_keys:
            if key in temporal_columns:
                schema.append((key, timestamp_type))
                continue

            sample = first_non_null_values.get(key)
            if target_db == "bigquery":
                db_type = DataTypeMapper.python_to_bigquery(sample)
            else:
                db_type = DataTypeMapper.python_to_postgres(sample)
            schema.append((key, db_type))
        return schema

    def _build_extract_result(
        self,
        *,
        load_config: Any,
        spec: FasttrackResourceSpec,
        rows_iter: Iterator[dict[str, Any]],
        force_full_refresh: bool,
        state: dict[str, Any] | None,
    ) -> ExtractResult:
        try:
            first_row = next(rows_iter)
        except StopIteration:
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=state,
                force_full_refresh=force_full_refresh,
            )

        preview_rows = [first_row, *itertools.islice(rows_iter, self.SCHEMA_PREVIEW_ROWS - 1)]
        schema = self._detect_fasttrack_schema(
            spec=spec,
            preview_rows=preview_rows,
            parse_temporal=self._should_parse_temporal_fields(load_config),
        )
        artifact = StreamingRowsArtifact(
            iterator=itertools.chain(preview_rows, rows_iter),
            batch_size=self._resolve_batch_size(load_config),
        )
        return ExtractResult(
            artifact=artifact,
            schema=schema,
            state=state,
            force_full_refresh=force_full_refresh,
        )
