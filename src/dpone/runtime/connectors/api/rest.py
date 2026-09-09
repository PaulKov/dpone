"""Generic JSON REST connector."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class RestCredentials:
    endpoint: str
    token: str | None = None
    api_key: str | None = None
    username: str | None = None
    password: str | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RestCredentials:
        return cls(
            endpoint=str(data.get("endpoint") or data.get("base_url") or ""),
            token=data.get("token") or data.get("bearer_token"),
            api_key=data.get("api_key"),
            username=data.get("username") or data.get("user"),
            password=data.get("password"),
        )

    @classmethod
    def from_credentials(cls, credentials: Any) -> RestCredentials:
        params = getattr(credentials, "additional_params", None) or {}
        return cls(
            endpoint=str(
                getattr(credentials, "endpoint", None) or params.get("endpoint") or params.get("base_url") or ""
            ),
            token=getattr(credentials, "token", None) or params.get("token") or params.get("bearer_token"),
            api_key=getattr(credentials, "api_key", None) or params.get("api_key"),
            username=getattr(credentials, "username", None) or params.get("username") or params.get("user"),
            password=getattr(credentials, "password", None) or params.get("password"),
        )

    @classmethod
    def from_vault(cls, vault_path: str, vault_manager=None, mount_point: str = "secret") -> RestCredentials:
        if vault_manager is None:
            from vault_kv_client import get_default_manager

            vault_manager = get_default_manager()
        return cls.from_dict(vault_manager.get_secret(mount_point=mount_point, path=vault_path))


class GenericRestConnector:
    """Pulls JSON records from configurable GET/POST REST APIs."""

    def __init__(
        self, credentials: RestCredentials | None = None, *, timeout: int = 60, session: requests.Session | None = None
    ):
        self.credentials = credentials or RestCredentials(endpoint="")
        self.timeout = timeout
        self.session = session or requests.Session()

    @classmethod
    def from_vault(cls, vault_path: str, **kwargs) -> GenericRestConnector:
        mount_point = kwargs.pop("vault_mount_point", "secret")
        return cls(RestCredentials.from_vault(vault_path, mount_point=mount_point), **kwargs)

    @classmethod
    def from_credentials(cls, credentials: Any, **kwargs) -> GenericRestConnector:
        return cls(RestCredentials.from_credentials(credentials), **kwargs)

    def iter_rows(
        self, options: Mapping[str, Any], last_state: Mapping[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        url = self._url(options)
        method = str(options.get("method", "GET")).upper()
        params = dict(options.get("params") or {})
        body = dict(options.get("json") or options.get("body") or {})
        headers = self._headers(options)
        auth = self._auth()
        pagination = dict(options.get("pagination") or {})
        records_path = str(options.get("records_path", "data"))
        cursor_param = pagination.get("cursor_param", "cursor")
        next_cursor_path = pagination.get("next_cursor_path", "next_cursor")
        limit = int(pagination.get("max_pages", 1000))
        page = int(pagination.get("start_page", 1))
        offset = int(pagination.get("start_offset", 0))
        cursor = (last_state or {}).get("cursor") or pagination.get("initial_cursor")

        for _ in range(limit):
            request_params = dict(params)
            if pagination.get("type") == "page":
                request_params[pagination.get("page_param", "page")] = page
                page += 1
            elif pagination.get("type") == "offset":
                request_params[pagination.get("offset_param", "offset")] = offset
                offset += int(pagination.get("limit", options.get("limit", 1000)))
                if pagination.get("limit_param"):
                    request_params[pagination["limit_param"]] = pagination.get("limit", options.get("limit", 1000))
            elif pagination.get("type") == "cursor" and cursor:
                request_params[cursor_param] = cursor

            response = self.session.request(
                method,
                url,
                params=request_params if method == "GET" else request_params,
                json=body if method != "GET" else None,
                headers=headers,
                auth=auth,
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
            records = self._extract_path(payload, records_path)
            if isinstance(records, dict):
                records = [records]
            if not records:
                break
            for record in records:
                if isinstance(record, Mapping):
                    yield dict(record)
                else:
                    yield {"value": record}

            if not pagination:
                break
            if pagination.get("type") == "cursor":
                cursor = self._extract_path(payload, str(next_cursor_path))
                if not cursor:
                    break
            if pagination.get("type") in {"page", "offset"} and len(records) == 0:
                break

    def _url(self, options: Mapping[str, Any]) -> str:
        endpoint = str(options.get("endpoint") or self.credentials.endpoint)
        path = str(options.get("path", ""))
        if path and not endpoint.endswith("/") and not path.startswith("/"):
            return f"{endpoint}/{path}"
        return f"{endpoint}{path}"

    def _headers(self, options: Mapping[str, Any]) -> dict[str, str]:
        headers = {str(k): str(v) for k, v in dict(options.get("headers") or {}).items()}
        if self.credentials.token:
            headers.setdefault("Authorization", f"Bearer {self.credentials.token}")
        if self.credentials.api_key:
            key_header = str(options.get("api_key_header", "X-API-Key"))
            headers.setdefault(key_header, self.credentials.api_key)
        return headers

    def _auth(self):
        if self.credentials.username and self.credentials.password:
            return (self.credentials.username, self.credentials.password)
        return None

    @staticmethod
    def _extract_path(payload: Any, path: str) -> Any:
        if not path or path == ".":
            return payload
        value = payload
        for part in path.split("."):
            if isinstance(value, Mapping):
                value = value.get(part)
            else:
                return None
        return value
