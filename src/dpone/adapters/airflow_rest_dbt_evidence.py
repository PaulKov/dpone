"""Bounded Airflow REST adapter for trusted dbt evidence campaigns."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from http.client import HTTPMessage, HTTPResponse
from typing import IO, Any
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from dpone.ports.dbt_airflow_evidence import (
    DbtAirflowEvidenceConfigurationError,
    DbtAirflowEvidencePortError,
)

_MAX_RESPONSE_BYTES = 1024 * 1024
_ALLOWED_API_VERSIONS = frozenset({"v1", "v2"})
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


class AirflowRestDbtEvidenceAdapter:
    """Call only the DAG-run endpoints needed by the campaign service."""

    def __init__(
        self,
        *,
        base_url: str,
        api_version: str,
        bearer_token: str,
        request_timeout_seconds: int = 10,
        opener: Callable[..., HTTPResponse] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._base_url = _base_url(base_url)
        if api_version not in _ALLOWED_API_VERSIONS:
            raise DbtAirflowEvidenceConfigurationError("Airflow API version is unsupported")
        if (
            not bearer_token
            or bearer_token.strip() != bearer_token
            or len(bearer_token) > 8_192
            or any(ord(character) < 32 or ord(character) == 127 for character in bearer_token)
        ):
            raise DbtAirflowEvidenceConfigurationError("Airflow API bearer token is invalid")
        if (
            isinstance(request_timeout_seconds, bool)
            or not isinstance(request_timeout_seconds, int)
            or not 1 <= request_timeout_seconds <= 60
        ):
            raise DbtAirflowEvidenceConfigurationError("Airflow API request timeout is invalid")
        self._api_version = api_version
        self._token = bearer_token
        self._timeout = request_timeout_seconds
        self._opener = opener or _open_without_redirects
        self._monotonic = monotonic

    def trigger(
        self,
        *,
        dag_id: str,
        dag_run_id: str,
        conf: Mapping[str, object],
        timeout_seconds: float | None = None,
    ) -> None:
        deadline = _deadline(
            remaining=timeout_seconds,
            monotonic=self._monotonic,
        )
        path = self._dag_run_path(dag_id, dag_run_id=None)
        payload = {
            "dag_run_id": _identity(dag_run_id, "DAG run"),
            "conf": dict(conf),
        }
        try:
            response = self._request(
                method="POST",
                path=path,
                payload=payload,
                timeout_seconds=_remaining(
                    deadline=deadline,
                    monotonic=self._monotonic,
                ),
            )
        except _Conflict:
            existing = self._request(
                method="GET",
                path=self._dag_run_path(dag_id, dag_run_id=dag_run_id),
                timeout_seconds=_remaining(
                    deadline=deadline,
                    monotonic=self._monotonic,
                ),
            )
            if existing.get("dag_run_id") != dag_run_id or existing.get("conf") != dict(conf):
                raise DbtAirflowEvidencePortError(
                    "existing Airflow DAG run differs from the campaign request"
                ) from None
            return
        except _Unavailable as exc:
            try:
                existing = self._request(
                    method="GET",
                    path=self._dag_run_path(
                        dag_id,
                        dag_run_id=dag_run_id,
                    ),
                    timeout_seconds=_remaining(
                        deadline=deadline,
                        monotonic=self._monotonic,
                    ),
                )
            except DbtAirflowEvidencePortError:
                raise exc from None
            if existing.get("dag_run_id") == dag_run_id and existing.get("conf") == dict(conf):
                return
            raise DbtAirflowEvidencePortError("existing Airflow DAG run differs from the campaign request") from None
        if response.get("dag_run_id") not in {None, dag_run_id}:
            raise DbtAirflowEvidencePortError("Airflow created a different DAG-run identity")

    def state(
        self,
        *,
        dag_id: str,
        dag_run_id: str,
        timeout_seconds: float | None = None,
    ) -> str:
        response = self._request(
            method="GET",
            path=self._dag_run_path(dag_id, dag_run_id=dag_run_id),
            timeout_seconds=timeout_seconds,
        )
        state = response.get("state")
        if response.get("dag_run_id") not in {None, dag_run_id} or not isinstance(state, str) or not state:
            raise DbtAirflowEvidencePortError("Airflow DAG-run response is invalid")
        return state.lower()

    def _dag_run_path(
        self,
        dag_id: str,
        *,
        dag_run_id: str | None,
    ) -> str:
        path = f"/api/{self._api_version}/dags/{quote(_identity(dag_id, 'DAG'), safe='')}/dagRuns"
        if dag_run_id is not None:
            path += f"/{quote(_identity(dag_run_id, 'DAG run'), safe='')}"
        return path

    def _request(
        self,
        *,
        method: str,
        path: str,
        payload: Mapping[str, object] | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        body = (
            json.dumps(
                dict(payload),
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            if payload is not None
            else None
        )
        request = Request(
            self._base_url + path,
            data=body,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._token}",
                **({"Content-Type": "application/json"} if body is not None else {}),
            },
            method=method,
        )
        try:
            with self._opener(
                request,
                timeout=_effective_timeout(
                    configured=self._timeout,
                    remaining=timeout_seconds,
                ),
            ) as response:
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
        except HTTPError as exc:
            if exc.code == 409 and method == "POST":
                raise _Conflict from None
            if method == "POST" and 500 <= exc.code <= 599:
                raise _Unavailable("Airflow API trigger outcome is ambiguous") from None
            raise DbtAirflowEvidencePortError("Airflow API rejected the evidence campaign request") from None
        except OSError:
            raise _Unavailable("Airflow API is unavailable") from None
        if len(raw) > _MAX_RESPONSE_BYTES:
            raise DbtAirflowEvidencePortError("Airflow API response exceeds the safety limit")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise DbtAirflowEvidencePortError("Airflow API response is not valid JSON") from None
        if not isinstance(value, dict):
            raise DbtAirflowEvidencePortError("Airflow API response is not a JSON object")
        return value


class _Conflict(RuntimeError):
    pass


class _Unavailable(DbtAirflowEvidencePortError):
    pass


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


def _open_without_redirects(
    request: Request,
    *,
    timeout: float,
) -> HTTPResponse:
    return build_opener(_NoRedirectHandler()).open(
        request,
        timeout=timeout,
    )


def _base_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        not value
        or parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or (parsed.scheme == "http" and parsed.hostname not in _LOOPBACK_HOSTS)
    ):
        raise DbtAirflowEvidenceConfigurationError("Airflow API URL must be HTTPS or loopback HTTP")
    path = parsed.path.rstrip("/")
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            path,
            "",
            "",
        )
    )


def _identity(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 250
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise DbtAirflowEvidencePortError(f"Airflow {field} identity is invalid")
    return value


def _effective_timeout(
    *,
    configured: int,
    remaining: float | None,
) -> float:
    if remaining is None:
        return float(configured)
    if isinstance(remaining, bool) or not isinstance(remaining, (int, float)) or remaining <= 0:
        raise DbtAirflowEvidencePortError("Airflow evidence campaign deadline expired")
    return min(float(configured), float(remaining))


def _deadline(
    *,
    remaining: float | None,
    monotonic: Callable[[], float],
) -> float | None:
    if remaining is None:
        return None
    _effective_timeout(configured=60, remaining=remaining)
    return monotonic() + float(remaining)


def _remaining(
    *,
    deadline: float | None,
    monotonic: Callable[[], float],
) -> float | None:
    if deadline is None:
        return None
    return deadline - monotonic()


__all__ = [
    "AirflowRestDbtEvidenceAdapter",
    "DbtAirflowEvidenceConfigurationError",
]
