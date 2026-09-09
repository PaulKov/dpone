from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vault_kv_client import VaultManager
else:
    VaultManager = Any
from urllib.parse import quote

from dpone.runtime.connectors.api.base import (
    AbstractAPIConnector,
    APIRateLimitConfig,
    APIRetryConfig,
)
from dpone.runtime.connectors.api.google_sheets_credentials import (
    GoogleSheetsCredentials,
    _load_google_auth_modules,
    _optional_int,
    _optional_str,
    get_default_manager,
)
from dpone.runtime.connectors.api.google_sheets_resources import (
    GoogleSheetsResourceSpec,
    list_google_sheets_resources,
)
from dpone.runtime.connectors.api.google_sheets_rows import GoogleSheetsRowsMixin
from dpone.runtime.connectors.api.google_sheets_transport import (
    download_published_csv,
    get_access_token,
    get_google_credentials,
    request_json,
)

logger = logging.getLogger(__name__)


class GoogleSheetsConnector(GoogleSheetsRowsMixin, AbstractAPIConnector):
    DEFAULT_RATE_LIMIT = APIRateLimitConfig(
        requests_per_second=5.0,
        burst_limit=10,
    )

    _SPREADSHEET_ID_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9-_]+)")

    def __init__(
        self,
        credentials: GoogleSheetsCredentials,
        retry_config: APIRetryConfig | None = None,
        rate_limit_config: APIRateLimitConfig | None = None,
        timeout: int = 60,
    ) -> None:
        super().__init__(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config or self.DEFAULT_RATE_LIMIT,
            timeout=timeout,
        )
        self._google_credentials: Any | None = None

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
        mount_point: str | None = None,
        *,
        rate_limit_delay: float | None = None,
        max_retries: int | None = None,
        timeout: int = 60,
        **_: Any,
    ) -> GoogleSheetsConnector:
        credentials = GoogleSheetsCredentials.from_vault(
            vault_path=vault_path,
            vault_manager=vault_manager,
            mount_point=mount_point,
        )
        retry_config = APIRetryConfig(max_retries=max_retries) if max_retries is not None else None
        rate_limit_config = None
        if rate_limit_delay is not None:
            rate_limit_config = APIRateLimitConfig(
                requests_per_second=1.0 / rate_limit_delay if rate_limit_delay > 0 else 5.0,
                burst_limit=10,
            )
        return cls(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config,
            timeout=timeout,
        )

    def get_resource_specs(self) -> list[GoogleSheetsResourceSpec]:
        return list_google_sheets_resources()

    def health_check(
        self,
        spreadsheet_id: str | None = None,
        spreadsheet_url: str | None = None,
        worksheet_gid: str | int | None = None,
    ) -> bool:
        try:
            if self.credentials.auth_type == "published_csv":
                if spreadsheet_id or spreadsheet_url:
                    resolved_id = self._resolve_spreadsheet_id(
                        spreadsheet_id=spreadsheet_id, spreadsheet_url=spreadsheet_url
                    )
                    self._download_published_csv(spreadsheet_id=resolved_id, worksheet_gid=worksheet_gid)
                return True
            self._get_access_token()
            return True
        except Exception:
            logger.exception("Google Sheets health_check failed")
            return False

    def get_resources(
        self,
        resource_type: str = "worksheets",
        filters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        params = dict(filters or {})
        params.update(kwargs)

        if resource_type == "worksheet_rows":
            return self.get_records(
                spreadsheet_id=params.get("spreadsheet_id"),
                spreadsheet_url=params.get("spreadsheet_url"),
                worksheet_title=params.get("worksheet_title"),
                worksheet_index=params.get("worksheet_index"),
                worksheet_gid=params.get("worksheet_gid"),
                range_name=params.get("range_name"),
                header_row=int(params.get("header_row", 1) or 1),
                skip_rows=int(params.get("skip_rows", 0) or 0),
                limit_rows=_optional_int(params.get("limit_rows")),
                limit_columns=_optional_int(params.get("limit_columns")),
                add_metadata_columns=bool(params.get("add_metadata_columns", True)),
                value_render_option=str(params.get("value_render_option", "UNFORMATTED_VALUE")),
                date_time_render_option=str(params.get("date_time_render_option", "FORMATTED_STRING")),
            )

        if self.credentials.auth_type == "published_csv":
            raise NotImplementedError(
                "get_resources() не поддержан для auth_type='published_csv'. "
                "Для smoke test используйте get_records() c worksheet_gid."
            )

        resolved_spreadsheet_id = self._resolve_spreadsheet_id(
            spreadsheet_id=params.get("spreadsheet_id"),
            spreadsheet_url=params.get("spreadsheet_url"),
        )
        metadata = self._get_spreadsheet_metadata(resolved_spreadsheet_id)

        resources: list[dict[str, Any]] = []
        for sheet in metadata.get("sheets", []):
            props = sheet.get("properties", {}) or {}
            grid = props.get("gridProperties", {}) or {}
            resources.append(
                {
                    "spreadsheet_id": resolved_spreadsheet_id,
                    "spreadsheet_title": metadata.get("properties", {}).get("title"),
                    "worksheet_id": props.get("sheetId"),
                    "worksheet_title": props.get("title"),
                    "worksheet_index": props.get("index"),
                    "row_count": grid.get("rowCount"),
                    "column_count": grid.get("columnCount"),
                }
            )
        return resources

    def get_records(
        self,
        spreadsheet_id: str | None = None,
        spreadsheet_url: str | None = None,
        worksheet_title: str | None = None,
        worksheet_index: int | None = None,
        worksheet_gid: str | int | None = None,
        range_name: str | None = None,
        header_row: int = 1,
        skip_rows: int = 0,
        limit_rows: int | None = None,
        limit_columns: int | None = None,
        add_metadata_columns: bool = True,
        value_render_option: str = "UNFORMATTED_VALUE",
        date_time_render_option: str = "FORMATTED_STRING",
    ) -> list[dict[str, Any]]:
        resolved_spreadsheet_id = self._resolve_spreadsheet_id(
            spreadsheet_id=spreadsheet_id,
            spreadsheet_url=spreadsheet_url,
        )

        if self.credentials.auth_type == "published_csv":
            raw_values = self._download_published_csv(
                spreadsheet_id=resolved_spreadsheet_id,
                worksheet_gid=worksheet_gid,
            )
            resolved_worksheet_title = worksheet_title or f"gid_{worksheet_gid}"
            spreadsheet_title = None
        else:
            metadata = self._get_spreadsheet_metadata(resolved_spreadsheet_id)
            resolved_worksheet_title = self._resolve_worksheet_title(
                metadata=metadata,
                worksheet_title=worksheet_title,
                worksheet_index=worksheet_index,
            )
            a1_range = self._build_a1_range(
                worksheet_title=resolved_worksheet_title,
                range_name=range_name,
            )
            values_payload = self._get_values(
                spreadsheet_id=resolved_spreadsheet_id,
                a1_range=a1_range,
                value_render_option=value_render_option,
                date_time_render_option=date_time_render_option,
            )
            raw_values = values_payload.get("values", []) or []
            spreadsheet_title = metadata.get("properties", {}).get("title")

        records = self._values_to_records(
            raw_values=raw_values,
            spreadsheet_id=resolved_spreadsheet_id,
            spreadsheet_title=spreadsheet_title,
            worksheet_title=resolved_worksheet_title,
            range_name=range_name,
            header_row=header_row,
            skip_rows=skip_rows,
            limit_rows=limit_rows,
            limit_columns=limit_columns,
            add_metadata_columns=add_metadata_columns,
        )

        self.logger.info(
            "GoogleSheets read completed: spreadsheet_id=%s worksheet=%s range=%s rows=%s requests=%s auth_type=%s",
            resolved_spreadsheet_id,
            resolved_worksheet_title,
            range_name or "<full worksheet/csv>",
            len(records),
            self._request_count,
            self.credentials.auth_type,
        )
        return records

    def _get_google_credentials(self) -> Any:
        return get_google_credentials(self, _load_google_auth_modules)

    def _get_access_token(self) -> str:
        return get_access_token(self, _load_google_auth_modules)

    @property
    def google_auth_loader(self) -> Any:
        return _load_google_auth_modules

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return request_json(self, method, url, params=params)

    def _download_published_csv(self, spreadsheet_id: str, worksheet_gid: str | int | None) -> list[list[Any]]:
        return download_published_csv(self, spreadsheet_id, worksheet_gid)

    def _get_spreadsheet_metadata(self, spreadsheet_id: str) -> dict[str, Any]:
        url = f"{self.credentials.endpoint.rstrip('/')}/v4/spreadsheets/{spreadsheet_id}"
        return self._request_json("GET", url, params={"includeGridData": "false"})

    def _get_values(
        self,
        spreadsheet_id: str,
        a1_range: str,
        value_render_option: str,
        date_time_render_option: str,
    ) -> dict[str, Any]:
        encoded_range = quote(a1_range, safe="!:$'")
        url = f"{self.credentials.endpoint.rstrip('/')}/v4/spreadsheets/{spreadsheet_id}/values/{encoded_range}"
        return self._request_json(
            "GET",
            url,
            params={
                "valueRenderOption": value_render_option,
                "dateTimeRenderOption": date_time_render_option,
            },
        )

    def _resolve_spreadsheet_id(self, spreadsheet_id: str | None, spreadsheet_url: str | None) -> str:
        if spreadsheet_id:
            return spreadsheet_id
        if spreadsheet_url:
            match = self._SPREADSHEET_ID_RE.search(spreadsheet_url)
            if not match:
                raise ValueError(f"Не удалось извлечь spreadsheet_id из URL: {spreadsheet_url}")
            return match.group(1)
        raise ValueError("Необходимо указать spreadsheet_id или spreadsheet_url")

    def _resolve_worksheet_title(
        self,
        metadata: dict[str, Any],
        worksheet_title: str | None,
        worksheet_index: int | None,
    ) -> str:
        sheets = metadata.get("sheets", []) or []
        if worksheet_title:
            return worksheet_title
        if worksheet_index is not None:
            for sheet in sheets:
                props = sheet.get("properties", {}) or {}
                if props.get("index") == worksheet_index:
                    return str(props["title"])
            raise ValueError(f"Worksheet с index={worksheet_index} не найден")
        if not sheets:
            raise ValueError("В spreadsheet не найдено ни одного worksheet")
        return str((sheets[0].get("properties", {}) or {}).get("title"))

    @staticmethod
    def _escape_sheet_title(title: str) -> str:
        return "'" + title.replace("'", "''") + "'"

    def _build_a1_range(self, worksheet_title: str, range_name: str | None) -> str:
        escaped_title = self._escape_sheet_title(worksheet_title)
        if not range_name:
            return escaped_title
        if "!" in range_name:
            return range_name
        return f"{escaped_title}!{range_name}"


__all__ = [
    "GoogleSheetsConnector",
    "GoogleSheetsCredentials",
    "_load_google_auth_modules",
    "_optional_int",
    "_optional_str",
    "get_default_manager",
]
