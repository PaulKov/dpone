from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, cast

import requests as _requests

from dpone.config.env import get_env_code
from dpone.runtime.connectors.api.base import (
    AbstractAPIConnector,
    APICredentials,
    APIRateLimitConfig,
    APIRetryConfig,
    ConcurrencyConfig,
    resolve_generic_api_token,
)
from dpone.runtime.connectors.api.mindbox_dates import build_mindbox_date_body
from dpone.runtime.connectors.api.mindbox_records import normalize_mindbox_record
from dpone.runtime.connectors.api.mindbox_resources import get_mindbox_resource, list_mindbox_resources
from dpone.runtime.connectors.api.mindbox_streams import _PrependedBytesIO

if TYPE_CHECKING:
    from vault_kv_client import VaultManager

from dpone.runtime.connectors.api.mindbox_resources import MindboxResourceSpec

logger = logging.getLogger(__name__)


def get_default_manager() -> VaultManager:
    try:
        from vault_kv_client import get_default_manager as load_default_manager
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("vault-kv-client is not installed") from exc
    return load_default_manager()


@dataclass
class MindboxCredentials(APICredentials):
    """Credentials for Mindbox Pull API."""

    endpoint_id: str = ""
    auth_type: str = field(default="mindbox_secret_key", init=False)

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
    ) -> MindboxCredentials:
        vm = vault_manager or get_default_manager()
        mount_point = get_env_code()
        secret = vm.get_secret(mount_point=mount_point, path=vault_path)
        endpoint_id = str(secret.get("endpoint_id") or "").strip()
        if not endpoint_id:
            raise ValueError("Mindbox credentials require an explicit non-empty endpoint_id")
        return cls(
            endpoint=secret["endpoint"],
            api_key=resolve_generic_api_token(secret, context="Mindbox credentials"),
            endpoint_id=endpoint_id,
        )

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> MindboxCredentials:
        endpoint_id = str(config.get("endpoint_id") or "").strip()
        if not endpoint_id:
            raise ValueError("Mindbox credentials require an explicit non-empty endpoint_id")
        return cls(
            endpoint=str(config["endpoint"]),
            api_key=resolve_generic_api_token(config, context="Mindbox credentials"),
            endpoint_id=endpoint_id,
        )

    def get_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f'Mindbox secretKey="{self.api_key}"',
            **dict(self.extra_headers or {}),
        }


class MindboxConnector(AbstractAPIConnector):
    """Connector for Mindbox Pull API async exports."""

    DEFAULT_RATE_LIMIT = APIRateLimitConfig(requests_per_second=2.0, burst_limit=5)
    DEFAULT_POLL_INTERVAL = 30
    DEFAULT_EXPORT_TIMEOUT = 3600
    DEFAULT_TIMEOUT = 300

    def __init__(
        self,
        credentials: MindboxCredentials,
        retry_config: APIRetryConfig | None = None,
        rate_limit_config: APIRateLimitConfig | None = None,
        concurrency_config: ConcurrencyConfig | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        super().__init__(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config or self.DEFAULT_RATE_LIMIT,
            concurrency_config=concurrency_config or ConcurrencyConfig(),
            timeout=timeout,
        )
        self._endpoint_id = credentials.endpoint_id

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
        *,
        rate_limit_delay: float | None = None,
        max_retries: int | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> MindboxConnector:
        credentials = MindboxCredentials.from_vault(vault_path=vault_path, vault_manager=vault_manager)
        retry_config = APIRetryConfig(max_retries=max_retries) if max_retries is not None else None
        rate_limit_config = None
        if rate_limit_delay is not None:
            rate_limit_config = APIRateLimitConfig(
                requests_per_second=1.0 / rate_limit_delay if rate_limit_delay > 0 else 10.0,
                burst_limit=5,
            )
        return cls(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config,
            timeout=timeout,
        )

    def get_resource_specs(self) -> list[MindboxResourceSpec]:
        return list_mindbox_resources()

    def _build_operation_url(self, operation: str) -> str:
        base = self.credentials.endpoint.rstrip("/")
        return f"{base}/v3/operations/sync?endpointId={self._endpoint_id}&operation={operation}"

    def start_export(self, operation: str, body: Mapping[str, Any] | None = None) -> str:
        url = self._build_operation_url(operation)
        self._apply_rate_limit()
        response = self.session.post(url, json=dict(body or {}), timeout=self.timeout)
        if not response.ok:
            self.logger.warning(
                "Mindbox export start error: %s %s",
                response.status_code,
                response.text[:500],
            )
        response.raise_for_status()
        data = response.json()
        export_id = data.get("exportId")
        if not export_id:
            raise RuntimeError(f"Mindbox не вернул exportId. Ответ: {json.dumps(data, ensure_ascii=False)[:500]}")
        self.logger.info("Mindbox export started: operation=%s exportId=%s", operation, export_id)
        return str(export_id)

    def poll_export(
        self,
        operation: str,
        export_id: str,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        timeout: int = DEFAULT_EXPORT_TIMEOUT,
    ) -> dict[str, Any]:
        url = self._build_operation_url(operation)
        body = {"exportId": export_id}
        start_time = time.monotonic()
        attempts = 0
        while True:
            elapsed = time.monotonic() - start_time
            if elapsed > timeout:
                raise TimeoutError(f"Mindbox экспорт {export_id} не завершился за {timeout}с ({attempts} попыток)")
            self._apply_rate_limit()
            response = self.session.post(url, json=body, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            attempts += 1
            export_result = dict(data.get("exportResult") or {})
            status = export_result.get("processingStatus", "Unknown")
            if status == "Ready":
                self.logger.info(
                    "Mindbox export ready: exportId=%s attempts=%s elapsed=%ss files=%s",
                    export_id,
                    attempts,
                    int(elapsed),
                    len(export_result.get("urls") or []),
                )
                return export_result
            if status == "Cancelled":
                reason = export_result.get("cancellationReason", "N/A")
                raise RuntimeError(f"Mindbox экспорт {export_id} отменён: {reason}")
            self.logger.info(
                "Mindbox export waiting: exportId=%s status=%s attempt=%s sleep=%ss",
                export_id,
                status,
                attempts,
                poll_interval,
            )
            time.sleep(poll_interval)

    @staticmethod
    def _build_download_stream(response: _requests.Response):
        response.raw.decode_content = True
        first_bytes = response.raw.read(2)
        combined_stream = _PrependedBytesIO(first_bytes, response.raw)
        if first_bytes == b"\x1f\x8b":
            return gzip.GzipFile(fileobj=cast(Any, combined_stream))
        return combined_stream

    def download_json_file(self, url: str, data_key: str) -> Iterator[dict[str, Any]]:
        self.logger.info("Mindbox JSON download: %s", url[:120])
        with _requests.get(url, stream=True, timeout=(10, 600)) as response:
            response.raise_for_status()
            raw_stream = self._build_download_stream(response)
            try:
                import ijson
            except ImportError:  # pragma: no cover - fallback exercised in tests/envs without ijson
                self.logger.warning("ijson не установлен; fallback на json.load() для Mindbox JSON")
                text_stream = io.TextIOWrapper(raw_stream, encoding="utf-8")
                payload = json.load(text_stream)
                for item in list(payload.get(data_key) or []):
                    yield self._normalize_record(item)
                return

            path = f"{data_key}.item"
            for item in ijson.items(raw_stream, path, use_float=True):
                yield self._normalize_record(item)

    def download_csv_file(self, url: str, delimiter: str = ";") -> Iterator[dict[str, Any]]:
        self.logger.info("Mindbox CSV download: %s", url[:120])
        with _requests.get(url, stream=True, timeout=(10, 600)) as response:
            response.raise_for_status()
            raw_stream = self._build_download_stream(response)
            text_stream = io.TextIOWrapper(raw_stream, encoding="utf-8-sig", newline="")
            reader = csv.DictReader(text_stream, delimiter=delimiter)
            for row in reader:
                yield self._normalize_record(dict(row))

    def run_export(
        self,
        operation: str,
        body: Mapping[str, Any] | None = None,
        data_key: str | None = None,
        fmt: str = "json",
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        export_timeout: int = DEFAULT_EXPORT_TIMEOUT,
        csv_delimiter: str = ";",
    ) -> Iterator[dict[str, Any]]:
        export_id = self.start_export(operation, body)
        export_result = self.poll_export(operation, export_id, poll_interval, export_timeout)
        urls = list(export_result.get("urls") or [])
        if not urls:
            self.logger.warning("Mindbox export %s returned no files", export_id)
            return
        total_records = 0
        for file_url in urls:
            if fmt == "csv":
                file_iter = self.download_csv_file(file_url, delimiter=csv_delimiter)
            else:
                if not data_key:
                    raise ValueError(f"data_key обязателен для JSON формата (operation={operation})")
                file_iter = self.download_json_file(file_url, data_key)
            for record in file_iter:
                total_records += 1
                yield record
        self.logger.info(
            "Mindbox export complete: operation=%s exportId=%s rows=%s files=%s",
            operation,
            export_id,
            total_records,
            len(urls),
        )

    @staticmethod
    def build_date_body(
        since: date | datetime | None = None,
        till: date | datetime | None = None,
        window_mode: str = "datetime_utc",
        utc_boundary_time: str = "21:00:00",
    ) -> dict[str, Any]:
        return build_mindbox_date_body(since, till, window_mode, utc_boundary_time)

    def _normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return normalize_mindbox_record(record)

    def _run_resource(
        self,
        resource_key: str,
        *,
        since: date | datetime | None = None,
        till: date | datetime | None = None,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        export_timeout: int = DEFAULT_EXPORT_TIMEOUT,
        utc_boundary_time: str = "21:00:00",
        csv_delimiter: str = ";",
    ) -> Iterator[dict[str, Any]]:
        spec = get_mindbox_resource(resource_key)
        body = self.build_date_body(since, till, spec.window_mode, utc_boundary_time)
        yield from self.run_export(
            spec.operation,
            body=body,
            data_key=spec.data_key,
            fmt=spec.format,
            poll_interval=poll_interval,
            export_timeout=export_timeout,
            csv_delimiter=csv_delimiter,
        )

    def get_resources(
        self,
        resource_type: str,
        filters: dict[str, Any] | None = None,
        **kwargs,
    ) -> Iterator[dict[str, Any]]:
        resource_type = str(resource_type).strip().lower()
        get_mindbox_resource(resource_type)
        merged = {**dict(filters or {}), **kwargs}
        since = merged.pop("since", None)
        till = merged.pop("till", None)
        poll_interval = int(merged.pop("poll_interval", self.DEFAULT_POLL_INTERVAL))
        export_timeout = int(merged.pop("export_timeout", self.DEFAULT_EXPORT_TIMEOUT))
        utc_boundary_time = str(merged.pop("utc_boundary_time", "21:00:00"))
        csv_delimiter = str(merged.pop("csv_delimiter", ";"))
        if merged:
            self.logger.warning("Mindbox get_resources ignored unsupported filters: %s", sorted(merged))
        yield from self._run_resource(
            resource_type,
            since=since,
            till=till,
            poll_interval=poll_interval,
            export_timeout=export_timeout,
            utc_boundary_time=utc_boundary_time,
            csv_delimiter=csv_delimiter,
        )

    def health_check(self) -> bool:
        try:
            url = self._build_operation_url("GetMailings")
            self._apply_rate_limit()
            response = self.session.post(url, json={}, timeout=30)
            response.raise_for_status()
            data = response.json()
            return "exportId" in data or "status" in data
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warning("Mindbox health check failed: %s", exc)
            return False


__all__ = [
    "MindboxConnector",
    "MindboxCredentials",
    "MindboxResourceSpec",
    "get_mindbox_resource",
    "list_mindbox_resources",
]
