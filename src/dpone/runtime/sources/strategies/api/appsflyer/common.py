from __future__ import annotations

import itertools
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Any

from dpone.runtime.connectors.api.appsflyer_resources import get_appsflyer_resource
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy


@dataclass(frozen=True)
class AppsflyerExtractWindow:
    resource: str
    app_ids: tuple[str, ...]
    start_date: date
    end_date: date
    timezone_name: str
    maximum_rows: int
    batch_size: int
    extra_params: dict[str, Any]
    lookback_partitions: tuple[str, ...]


class AppsflyerBaseExtractStrategy(APIBaseStrategy):
    DEFAULT_BATCH_SIZE = 5_000
    DEFAULT_MAXIMUM_ROWS = 1_000_000
    DEFAULT_TIMEZONE = "Europe/Moscow"
    DEFAULT_FULL_REFRESH_DAYS = 365

    def _resolve_resource(self, load_config: Any) -> str:
        options = self._get_options(load_config)
        resource = options.get("resource") or getattr(load_config, "source_table", None)
        if not resource:
            raise ValueError("Для AppsFlyer source.options.resource обязателен")
        get_appsflyer_resource(str(resource))
        return str(resource)

    def _resolve_app_ids(self, load_config: Any) -> tuple[str, ...]:
        options = self._get_options(load_config)
        raw = options.get("app_ids") or options.get("app_id")
        if raw is None:
            connector_default = getattr(getattr(self, "connector", None), "default_app_id", None)
            if connector_default:
                return (str(connector_default),)
            raise ValueError("Для AppsFlyer необходимо указать source.options.app_ids (или app_id)")

        if isinstance(raw, str):
            items = [item.strip() for item in raw.split(",") if item.strip()]
        elif isinstance(raw, Sequence):
            items = [str(item).strip() for item in raw if str(item).strip()]
        else:
            raise ValueError("source.options.app_ids должен быть строкой или списком")

        if not items:
            raise ValueError("source.options.app_ids не должен быть пустым")
        return tuple(items)

    def _resolve_timezone(self, load_config: Any) -> str:
        options = self._get_options(load_config)
        return str(options.get("timezone") or self.DEFAULT_TIMEZONE)

    def _resolve_maximum_rows(self, load_config: Any) -> int:
        options = self._get_options(load_config)
        return int(options.get("maximum_rows", self.DEFAULT_MAXIMUM_ROWS))

    def _resolve_batch_size(self, load_config: Any) -> int:
        options = self._get_options(load_config)
        return int(options.get("batch_size", self.DEFAULT_BATCH_SIZE))

    def _resolve_extra_params(self, load_config: Any) -> dict[str, Any]:
        options = dict(self._get_options(load_config) or {})
        ignore = {
            "resource",
            "app_ids",
            "app_id",
            "timezone",
            "maximum_rows",
            "batch_size",
            "date_from",
            "date_to",
            "lookback_days",
            "full_refresh_days",
            "incremental_column",
            "export_format",
            "compress_export",
            "unique_key",
            "micro_batch_commit",
            "only_new_rows",
            "log_sample_rows",
            "table_labels",
            "table_description",
            "technical_columns",
            "include_technical_columns",
            "apply_table_metadata",
        }
        return {key: value for key, value in options.items() if key not in ignore}

    def _build_partitions(self, start_date: date, end_date: date) -> tuple[str, ...]:
        days = (end_date - start_date).days
        return tuple((start_date + timedelta(days=offset)).isoformat() for offset in range(days + 1))

    def _normalize_date(self, value: Any) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        parsed = self.connector._parse_possible_datetime(value)
        if parsed is not None:
            return parsed.date()
        raise ValueError(f"Не удалось распарсить дату AppsFlyer: {value!r}")

    def _resolve_date_window(
        self,
        load_config: Any,
        last_state: dict[str, Any] | None,
        *,
        full_refresh: bool,
    ) -> tuple[date, date]:
        options = self._get_options(load_config)
        resource = get_appsflyer_resource(self._resolve_resource(load_config))

        explicit_from = options.get("date_from")
        explicit_to = options.get("date_to")
        if explicit_from and explicit_to:
            start_date = self._normalize_date(explicit_from)
            end_date = self._normalize_date(explicit_to)
            return start_date, end_date

        yesterday = date.today() - timedelta(days=1)

        if full_refresh:
            full_days = int(options.get("full_refresh_days", self.DEFAULT_FULL_REFRESH_DAYS))
            end_date = self._normalize_date(explicit_to) if explicit_to else yesterday
            start_date = (
                self._normalize_date(explicit_from)
                if explicit_from
                else end_date - timedelta(days=max(full_days - 1, 0))
            )
            return start_date, end_date

        lookback_days = int(options.get("lookback_days", resource.default_lookback_days))
        end_date = self._normalize_date(explicit_to) if explicit_to else yesterday
        if explicit_from:
            start_date = self._normalize_date(explicit_from)
            return start_date, end_date

        if last_state and last_state.get("last_value") is not None:
            last_value_date = self._normalize_date(last_state["last_value"])
            start_date = last_value_date - timedelta(days=lookback_days)
        else:
            start_date = end_date - timedelta(days=max(lookback_days - 1, 0))
        return start_date, end_date

    def _build_window(
        self,
        load_config: Any,
        last_state: dict[str, Any] | None,
        *,
        full_refresh: bool,
    ) -> AppsflyerExtractWindow:
        resource = self._resolve_resource(load_config)
        start_date, end_date = self._resolve_date_window(load_config, last_state, full_refresh=full_refresh)
        if start_date > end_date:
            raise ValueError(f"Некорректное окно AppsFlyer: {start_date} > {end_date}")
        return AppsflyerExtractWindow(
            resource=resource,
            app_ids=self._resolve_app_ids(load_config),
            start_date=start_date,
            end_date=end_date,
            timezone_name=self._resolve_timezone(load_config),
            maximum_rows=self._resolve_maximum_rows(load_config),
            batch_size=self._resolve_batch_size(load_config),
            extra_params=self._resolve_extra_params(load_config),
            lookback_partitions=self._build_partitions(start_date, end_date),
        )

    def _iter_window_rows(self, window: AppsflyerExtractWindow) -> Iterator[dict[str, Any]]:
        for app_id in window.app_ids:
            yield from self.connector.iter_resource_rows(
                resource_name=window.resource,
                app_id=app_id,
                from_value=window.start_date,
                to_value=window.end_date,
                timezone_name=window.timezone_name,
                maximum_rows=window.maximum_rows,
                extra_params=window.extra_params,
            )

    def _build_extract_result(
        self,
        *,
        load_config: Any,
        rows_iter: Iterator[dict[str, Any]],
        window: AppsflyerExtractWindow,
        state: dict[str, Any] | None,
        include_partitions: bool,
    ) -> ExtractResult:
        try:
            first_row = next(rows_iter)
        except StopIteration:
            empty_artifact = InMemoryRowsArtifact([])
            if include_partitions:
                empty_artifact.lookback_partitions = window.lookback_partitions
                empty_artifact.new_partitions = window.lookback_partitions
                empty_artifact.incremental_column = "date"
            return ExtractResult(artifact=empty_artifact, schema=[], state=state, force_full_refresh=False)

        schema = self._detect_schema_from_records([first_row])
        iterator = itertools.chain([first_row], rows_iter)
        artifact = self._build_artifact_from_iterator(iterator, batch_size=window.batch_size)
        if include_partitions:
            artifact.lookback_partitions = window.lookback_partitions
            artifact.new_partitions = window.lookback_partitions
            artifact.incremental_column = "date"
        self._log_extract_plan(load_config, window, schema)
        return ExtractResult(artifact=artifact, schema=schema, state=state, force_full_refresh=False)

    def _log_extract_plan(
        self,
        load_config: Any,
        window: AppsflyerExtractWindow,
        schema: Sequence[tuple[str, str]],
    ) -> None:
        details = {
            "Resource": window.resource,
            "Apps": list(window.app_ids),
            "From": window.start_date.isoformat(),
            "To": window.end_date.isoformat(),
            "Timezone": window.timezone_name,
            "Maximum_Rows": window.maximum_rows,
            "Partitions": list(window.lookback_partitions),
            "Schema_Columns": len(schema),
            "Target": f"{getattr(load_config, 'target_schema', '?')}.{getattr(load_config, 'target_table', '?')}",
        }
        if hasattr(self.logger, "log_etl_progress"):
            self.logger.log_etl_progress("APPSFLYER_EXTRACT_PLAN", details)
        else:
            self.logger.info("AppsFlyer extract plan: %s", details)

    def _get_state_load_config(self, load_config: Any) -> Any:
        options = dict(self._get_options(load_config) or {})
        options.setdefault("incremental_column", "date")
        if options == getattr(load_config, "options", {}):
            return load_config
        return replace(load_config, options=options)
