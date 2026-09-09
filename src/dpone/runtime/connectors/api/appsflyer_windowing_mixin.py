"""AppsFlyer windowing, parsing, and quota helpers."""

from __future__ import annotations

import csv
import re
from collections.abc import Iterator, Mapping
from datetime import date, datetime, time, timedelta
from io import StringIO
from typing import Any
from urllib.parse import urlsplit

import requests

from dpone.runtime.connectors.api.appsflyer_resources import AppsflyerResourceSpec, get_appsflyer_resource

_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_NORMALIZE_RE = re.compile(r"[^a-z0-9_]+")
_CANONICAL_EXPORT_PATH = "/api/raw-data/export/app"
_QUOTA_EXCEEDED_RE = re.compile(
    r"(maximum number of install reports that can be downloaded today|limit reached for daily-report)",
    re.IGNORECASE,
)


class AppsflyerQuotaExceededError(RuntimeError):
    """Known AppsFlyer vendor quota exhaustion for live report downloads."""


class AppsflyerWindowingMixin:
    def _iter_window_rows(
        self,
        *,
        spec: AppsflyerResourceSpec,
        app_id: str,
        start_dt: datetime,
        end_dt: datetime,
        timezone_name: str | None,
        maximum_rows: int,
        extra_params: Mapping[str, Any] | None,
    ) -> Iterator[dict[str, Any]]:
        rows = self._fetch_rows_for_window(
            spec=spec,
            app_id=app_id,
            start_dt=start_dt,
            end_dt=end_dt,
            timezone_name=timezone_name,
            maximum_rows=maximum_rows,
            extra_params=extra_params,
        )
        if self._should_split_window(len(rows), maximum_rows, start_dt, end_dt):
            left_end, right_start = self._split_window(start_dt, end_dt)
            self.logger.warning(
                "AppsFlyer window for %s/%s hit %s rows and will be split: %s -> %s | %s -> %s",
                app_id,
                spec.name,
                len(rows),
                self._format_datetime(start_dt),
                self._format_datetime(left_end),
                self._format_datetime(right_start),
                self._format_datetime(end_dt),
            )
            yield from self._iter_window_rows(
                spec=spec,
                app_id=app_id,
                start_dt=start_dt,
                end_dt=left_end,
                timezone_name=timezone_name,
                maximum_rows=maximum_rows,
                extra_params=extra_params,
            )
            yield from self._iter_window_rows(
                spec=spec,
                app_id=app_id,
                start_dt=right_start,
                end_dt=end_dt,
                timezone_name=timezone_name,
                maximum_rows=maximum_rows,
                extra_params=extra_params,
            )
            return
        if len(rows) >= maximum_rows and not self._can_split(start_dt, end_dt):
            raise ValueError(
                f"AppsFlyer resource '{spec.name}' for app_id '{app_id}' reached maximum_rows={maximum_rows} "
                "even after minimal chunking. Narrow the time range or reduce maximum_rows."
            )
        yield from rows

    def _fetch_rows_for_window(
        self,
        *,
        spec: AppsflyerResourceSpec,
        app_id: str,
        start_dt: datetime,
        end_dt: datetime,
        timezone_name: str | None,
        maximum_rows: int,
        extra_params: Mapping[str, Any] | None,
    ) -> list[dict[str, Any]]:
        endpoint = self._build_resource_endpoint(app_id, spec.name)
        params = self._build_query_params(
            spec=spec,
            start_dt=start_dt,
            end_dt=end_dt,
            timezone_name=timezone_name,
            maximum_rows=maximum_rows,
            extra_params=extra_params,
        )
        try:
            response = self._request("GET", endpoint, params=params, headers={"Accept": "text/csv"})
        except requests.HTTPError as exc:
            if self._is_quota_exceeded_error(exc):
                raise AppsflyerQuotaExceededError(self._quota_error_message(exc)) from exc
            raise
        return self._parse_csv_rows(
            response.text,
            app_id=app_id,
            resource_name=spec.name,
            start_dt=start_dt,
        )

    @staticmethod
    def _is_quota_exceeded_error(exc: requests.HTTPError) -> bool:
        response = getattr(exc, "response", None)
        if response is None:
            return False
        if response.status_code not in {400, 403, 429}:
            return False
        return bool(_QUOTA_EXCEEDED_RE.search(response.text or ""))

    @staticmethod
    def _quota_error_message(exc: requests.HTTPError) -> str:
        response = getattr(exc, "response", None)
        if response is None:
            return "AppsFlyer report quota is exhausted for the current app/token."
        detail = " ".join((response.text or "").strip().split())
        if detail:
            return (
                f"AppsFlyer report quota is exhausted for the current app/token. HTTP {response.status_code}: {detail}"
            )
        return f"AppsFlyer report quota is exhausted for the current app/token. HTTP {response.status_code}"

    def _build_resource_endpoint(self, app_id: str, resource_name: str) -> str:
        spec = get_appsflyer_resource(resource_name)
        if spec.endpoint_base_path != _CANONICAL_EXPORT_PATH:
            return f"{self._endpoint_origin()}{spec.endpoint_base_path}/{app_id}/{spec.endpoint_path}"
        return f"{app_id}/{spec.endpoint_path}"

    def _build_query_params(
        self,
        *,
        spec: AppsflyerResourceSpec,
        start_dt: datetime,
        end_dt: datetime,
        timezone_name: str | None,
        maximum_rows: int,
        extra_params: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = dict(self.credentials.extra_params or {})
        params.update(dict(extra_params or {}))
        if spec.uses_date_only_window:
            params["from"] = start_dt.date().isoformat()
            params["to"] = end_dt.date().isoformat()
        else:
            params["from"] = self._format_datetime(start_dt)
            params["to"] = self._format_datetime(end_dt)
        if spec.supports_maximum_rows:
            params["maximum_rows"] = int(maximum_rows)
        else:
            params.pop("maximum_rows", None)
        if timezone_name and spec.supports_timezone:
            params["timezone"] = timezone_name
        elif not spec.supports_timezone:
            params.pop("timezone", None)
        return {key: value for key, value in params.items() if value is not None}

    def _parse_csv_rows(
        self,
        csv_text: str,
        *,
        app_id: str,
        resource_name: str,
        start_dt: datetime,
    ) -> list[dict[str, Any]]:
        if not csv_text.strip():
            return []
        reader = csv.DictReader(StringIO(csv_text))
        rows: list[dict[str, Any]] = []
        fallback_date = start_dt.date().isoformat()
        for raw_row in reader:
            if raw_row is None:
                continue
            normalized: dict[str, Any] = {}
            for key, value in raw_row.items():
                if key is None:
                    continue
                normalized[self._normalize_column_name(key)] = self._normalize_value(value)
            normalized["source_app_id"] = app_id
            normalized["resource_name"] = resource_name
            normalized["date"] = self._derive_partition_date(normalized, fallback_date)
            rows.append(normalized)
        return rows

    @staticmethod
    def _normalize_column_name(name: str) -> str:
        name = name.strip().lower()
        name = name.replace("-", "_").replace(" ", "_").replace("/", "_")
        name = _NORMALIZE_RE.sub("_", name)
        name = re.sub(r"_+", "_", name).strip("_")
        return name

    @staticmethod
    def _normalize_value(value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if cleaned == "":
            return None
        return cleaned

    def _derive_partition_date(self, row: Mapping[str, Any], fallback_date: str) -> str:
        for key in (
            "date",
            "report_date",
            "event_date",
            "install_date",
            "touch_date",
            "click_date",
            "detect_date",
            "event_time",
            "install_time",
            "touch_time",
            "click_time",
            "detect_time",
        ):
            raw = row.get(key)
            parsed = self._parse_possible_datetime(raw)
            if parsed is not None:
                return parsed.date().isoformat()
        return fallback_date

    def _endpoint_origin(self) -> str:
        parsed = urlsplit(self.credentials.endpoint)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"AppsFlyer endpoint has no valid origin: {self.credentials.endpoint}")
        return f"{parsed.scheme}://{parsed.netloc}"

    @staticmethod
    def _parse_possible_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, time.min)
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            try:
                return datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                pass
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    @staticmethod
    def _coerce_from_value(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        if isinstance(value, date):
            return datetime.combine(value, time.min)
        text = str(value).strip()
        if _DATE_ONLY_RE.match(text):
            return datetime.strptime(text, "%Y-%m-%d")
        parsed = AppsflyerWindowingMixin._parse_possible_datetime(text)
        if parsed is None:
            raise ValueError(f"Не удалось распарсить from value: {value}")
        return parsed.replace(tzinfo=None)

    @staticmethod
    def _coerce_to_value(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        if isinstance(value, date):
            return datetime.combine(value, time.max.replace(microsecond=0))
        text = str(value).strip()
        if _DATE_ONLY_RE.match(text):
            return datetime.combine(datetime.strptime(text, "%Y-%m-%d").date(), time.max.replace(microsecond=0))
        parsed = AppsflyerWindowingMixin._parse_possible_datetime(text)
        if parsed is None:
            raise ValueError(f"Не удалось распарсить to value: {value}")
        return parsed.replace(tzinfo=None)

    @staticmethod
    def _format_datetime(value: datetime) -> str:
        return value.strftime("%Y-%m-%d %H:%M:%S")

    def _should_split_window(
        self,
        row_count: int,
        maximum_rows: int,
        start_dt: datetime,
        end_dt: datetime,
    ) -> bool:
        threshold = max(1, int(maximum_rows * self.row_limit_split_ratio))
        return row_count >= threshold and self._can_split(start_dt, end_dt)

    def _can_split(self, start_dt: datetime, end_dt: datetime) -> bool:
        return int((end_dt - start_dt).total_seconds()) >= self.MIN_SPLIT_SECONDS

    def _split_window(self, start_dt: datetime, end_dt: datetime) -> tuple[datetime, datetime]:
        total_seconds = int((end_dt - start_dt).total_seconds())
        if total_seconds < self.MIN_SPLIT_SECONDS:
            raise ValueError("AppsFlyer window is too small to split further")
        mid = start_dt + timedelta(seconds=total_seconds // 2)
        return mid, mid + timedelta(seconds=1)


__all__ = ["AppsflyerWindowingMixin"]
