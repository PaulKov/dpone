from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from dpone.runtime.sources.strategies.api.base import APIBaseStrategy


class SimilarwebBaseStrategy(APIBaseStrategy):
    """Shared monthly-snapshot and idempotency helpers for SimilarWeb strategies."""

    _SNAPSHOT_MONTH_FORMATS = ("%Y-%m-%d", "%Y-%m")

    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        del load_config
        return None

    def _resolve_period(self, options: dict[str, Any]) -> tuple[str, str, str]:
        explicit = options.get("snapshot_month")
        if explicit is not None:
            snapshot_month = self._parse_snapshot_month(explicit)
        else:
            snapshot_month = self._calculate_previous_month()
        start_date, end_date = self._month_bounds(snapshot_month)
        return snapshot_month, start_date, end_date

    def _parse_snapshot_month(self, value: Any) -> str:
        if isinstance(value, date):
            return value.replace(day=1).strftime("%Y-%m-%d")
        if not isinstance(value, str):
            raise ValueError(f"snapshot_month must be a string or date, got: {type(value).__name__}")
        raw = value.strip()
        for fmt in self._SNAPSHOT_MONTH_FORMATS:
            try:
                return datetime.strptime(raw, fmt).replace(day=1).strftime("%Y-%m-%d")
            except ValueError:
                continue
        raise ValueError(f"Invalid snapshot_month '{raw}'. Expected YYYY-MM-DD or YYYY-MM.")

    @staticmethod
    def _calculate_previous_month() -> str:
        today = date.today()
        last_prev = today.replace(day=1) - timedelta(days=1)
        return last_prev.replace(day=1).strftime("%Y-%m-%d")

    @staticmethod
    def _month_bounds(snapshot_month: str) -> tuple[str, str]:
        first_day = datetime.strptime(snapshot_month, "%Y-%m-%d").date()
        if first_day.month == 12:
            last_day = date(first_day.year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = date(first_day.year, first_day.month + 1, 1) - timedelta(days=1)
        return first_day.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d")

    @staticmethod
    def _resolve_domains(options: dict[str, Any]) -> list[str]:
        domains = options.get("domains")
        if domains is None:
            raise ValueError("SimilarWeb source options must define 'domains'")
        if isinstance(domains, str):
            resolved = [item.strip() for item in domains.split(",") if item.strip()]
            if not resolved:
                raise ValueError("SimilarWeb source option 'domains' must not be empty")
            return resolved
        if isinstance(domains, list | tuple):
            resolved = [str(item).strip() for item in domains if str(item).strip()]
            if not resolved:
                raise ValueError("SimilarWeb source option 'domains' must not be an empty list")
            return resolved
        raise ValueError("SimilarWeb source option 'domains' must be a string or list[str]")

    def _is_domain_month_loaded(self, load_config: Any, snapshot_month: str, domain: str) -> bool:
        if not self.sink_connector or not hasattr(self.sink_connector, "get_records"):
            return False
        try:
            fq_table = self._fq_table(load_config)
            query = f"""
            SELECT 1
            FROM {fq_table}
            WHERE date = DATE('{snapshot_month}')
              AND domain = '{domain}'
            LIMIT 1
            """
            rows = self.sink_connector.get_records(query, as_dict=True)
            return bool(rows)
        except Exception:
            return False

    def _delete_domain_month_data(self, load_config: Any, snapshot_month: str, domain: str) -> int:
        if not self.sink_connector or not hasattr(self.sink_connector, "execute_query"):
            return 0
        try:
            fq_table = self._fq_table(load_config)
            query = f"""
            DELETE FROM {fq_table}
            WHERE date = DATE('{snapshot_month}')
              AND domain = '{domain}'
            """
            result = self.sink_connector.execute_query(query)
            return int(result or 0)
        except Exception:
            return 0

    def _fq_table(self, load_config: Any) -> str:
        project_id = getattr(self.sink_connector, "project_id", None)
        if project_id:
            return f"`{project_id}.{load_config.target_schema}.{load_config.target_table}`"
        return f"{load_config.target_schema}.{load_config.target_table}"
