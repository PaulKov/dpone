"""Bounded stdlib HTTP adapter for the local Studio application service."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Mapping
from concurrent.futures import TimeoutError as FutureTimeoutError
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import monotonic
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import urlparse

from dpone.adapters.studio_http_errors import (
    StudioError,
    StudioHttpError,
    http_error,
    studio_error_status,
)
from dpone.adapters.studio_http_support import (
    dispatch_async,
    query_dict,
    request_method,
    server_authority,
)

if TYPE_CHECKING:
    from dpone.readiness.studio_http_models import StudioAuditLog, StudioHttpConfig


class StudioRequestRouter(Protocol):
    """Transport-facing subset of the Studio application router."""

    def allowed_methods(self, path: str) -> tuple[str, ...]: ...

    def is_public(self, method: str, path: str) -> bool: ...

    def is_deprecated(self, method: str, path: str) -> bool: ...

    def audit_path(self, method: str, path: str) -> str: ...

    def dispatch(
        self,
        *,
        method: str,
        path: str,
        query: Mapping[str, str],
        body: Mapping[str, Any],
    ) -> dict[str, Any]: ...


class StudioThreadingHTTPServer(ThreadingHTTPServer):
    """Local server with a bounded worker gate and enough accept backlog."""

    daemon_threads = True
    request_queue_size = 128


def build_studio_http_handler(
    *,
    router: StudioRequestRouter,
    config: StudioHttpConfig,
    audit: StudioAuditLog,
    logger: logging.Logger | None = None,
) -> type[BaseHTTPRequestHandler]:
    """Return a handler class without binding sockets or discovering services."""

    config.validate()
    semaphore = threading.BoundedSemaphore(config.max_concurrent_requests)
    adapter_logger = logger or logging.getLogger("dpone.studio")

    class Handler(BaseHTTPRequestHandler):
        server_version = "dpone-studio-local"
        sys_version = ""

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(config.request_timeout_seconds)

        def do_OPTIONS(self) -> None:  # noqa: N802 - stdlib API
            if not semaphore.acquire(blocking=False):
                self._send_error(http_error("DPONE_STUDIO_CONCURRENCY_LIMIT", 429))
                return
            try:
                try:
                    self._handle_options()
                except StudioError as exc:
                    self._send_error(exc)
            finally:
                semaphore.release()

        def do_GET(self) -> None:  # noqa: N802 - stdlib API
            self._handle_request("GET")

        def do_POST(self) -> None:  # noqa: N802 - stdlib API
            self._handle_request("POST")

        def do_PUT(self) -> None:  # noqa: N802 - stdlib API
            self._handle_request("PUT")

        def do_PATCH(self) -> None:  # noqa: N802 - stdlib API
            self._handle_request("PATCH")

        def do_DELETE(self) -> None:  # noqa: N802 - stdlib API
            self._handle_request("DELETE")

        def do_HEAD(self) -> None:  # noqa: N802 - stdlib API
            self._handle_request("HEAD")

        def do_TRACE(self) -> None:  # noqa: N802 - stdlib API
            self._handle_request("TRACE")

        def do_CONNECT(self) -> None:  # noqa: N802 - stdlib API
            self._handle_request("CONNECT")

        def send_error(
            self,
            code: int,
            message: str | None = None,
            explain: str | None = None,
        ) -> None:
            """Replace stdlib HTML parser/method errors with the public error contract."""

            del message, explain
            if getattr(self, "request_version", "HTTP/0.9") == "HTTP/0.9":
                self.request_version = "HTTP/1.0"
            if code == 501:
                error = StudioHttpError(
                    "DPONE_STUDIO_METHOD_NOT_ALLOWED",
                    "HTTP method is not allowed for this Studio endpoint.",
                    status=405,
                )
            else:
                error = StudioHttpError(
                    "DPONE_STUDIO_HTTP_REQUEST_INVALID",
                    "Studio HTTP request could not be parsed safely.",
                    status=400,
                )
            self._send_error(error)

        def _handle_request(self, method: str) -> None:
            if not semaphore.acquire(blocking=False):
                self._send_error(http_error("DPONE_STUDIO_CONCURRENCY_LIMIT", 429))
                return
            slot_owned_by_handler = True
            actor = "anonymous"
            try:
                started_at = monotonic()
                parsed = urlparse(self.path)
                self._require_allowed_origin()
                methods = router.allowed_methods(parsed.path)
                if method not in methods:
                    router.dispatch(
                        method=method,
                        path=parsed.path,
                        query={},
                        body={},
                    )
                actor = self._authorize(method, parsed.path)
                body = self._read_json() if method in {"POST", "PUT", "PATCH"} else {}
                remaining = config.request_timeout_seconds - (monotonic() - started_at)
                if remaining <= 0:
                    raise http_error("DPONE_STUDIO_REQUEST_TIMEOUT", 408)
                response_future = dispatch_async(
                    dispatch=router.dispatch,
                    method=method,
                    path=parsed.path,
                    query=query_dict(parsed.query),
                    body=body,
                    release_slot=semaphore.release,
                )
                slot_owned_by_handler = False
                try:
                    response = response_future.result(timeout=remaining)
                except FutureTimeoutError as exc:
                    raise http_error("DPONE_STUDIO_REQUEST_TIMEOUT", 408) from exc
                self._send_json(response, status=200, actor=actor)
            except StudioError as exc:
                self._send_error(exc, actor=actor)
            except TimeoutError:
                self._send_error(
                    http_error("DPONE_STUDIO_REQUEST_TIMEOUT", 408),
                    actor=actor,
                )
            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception:  # noqa: BLE001 - boundary hides internal failures.
                adapter_logger.error("Studio request failed with an internal error.")
                self._send_error(
                    http_error("DPONE_STUDIO_INTERNAL_ERROR", 500),
                    actor=actor,
                )
            finally:
                if slot_owned_by_handler:
                    semaphore.release()

        def _handle_options(self) -> None:
            parsed = urlparse(self.path)
            self._require_allowed_origin()
            methods = router.allowed_methods(parsed.path)
            if not methods:
                raise StudioError(
                    "DPONE_STUDIO_ROUTE_NOT_FOUND",
                    "Studio endpoint was not found.",
                    stage="studio_http",
                )
            self._send_json(
                {"ok": True, "methods": list(methods)},
                status=200,
                actor="preflight",
                allowed=methods,
            )

        def _authorize(self, method: str, path: str) -> str:
            if router.is_public(method, path) or not config.token:
                return "local_operator"
            bearer = self.headers.get("Authorization", "")
            candidate = bearer[7:] if bearer.startswith("Bearer ") else ""
            if not candidate:
                candidate = self.headers.get("X-Dpone-Studio-Token", "")
            if config.token_matches(candidate):
                return "shared_token"
            raise StudioError(
                "DPONE_STUDIO_UNAUTHORIZED",
                "A valid Studio bearer token is required.",
                stage="studio_http",
            )

        def _require_allowed_origin(self) -> None:
            origin = self.headers.get("Origin")
            request_host = self.headers.get("Host", "")
            trusted_authority = server_authority(self.server.server_address)
            if not config.request_host_allowed(request_host, trusted_authority):
                raise StudioError(
                    "DPONE_STUDIO_HOST_FORBIDDEN",
                    "Request Host is not allowed by Studio local binding policy.",
                    stage="studio_http",
                )
            if not config.origin_allowed(origin, trusted_authority):
                raise StudioError(
                    "DPONE_STUDIO_ORIGIN_FORBIDDEN",
                    "Request origin is not allowed by Studio CORS policy.",
                    stage="studio_http",
                )

        def _read_json(self) -> Mapping[str, Any]:
            content_type = self.headers.get("Content-Type", "").split(";", maxsplit=1)[0].strip().lower()
            if content_type != "application/json":
                raise StudioError(
                    "DPONE_STUDIO_MEDIA_TYPE_UNSUPPORTED",
                    "Studio request body must use application/json.",
                    stage="studio_http",
                )
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise StudioError(
                    "DPONE_STUDIO_CONTENT_LENGTH_INVALID",
                    "Content-Length must be a non-negative integer.",
                    stage="studio_http",
                ) from exc
            if length < 0:
                raise StudioError(
                    "DPONE_STUDIO_CONTENT_LENGTH_INVALID",
                    "Content-Length must be a non-negative integer.",
                    stage="studio_http",
                )
            if length > config.body_limit_bytes:
                raise StudioError(
                    "DPONE_STUDIO_BODY_TOO_LARGE",
                    "Studio request body exceeds the 1 MiB limit.",
                    stage="studio_http",
                )
            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise StudioError(
                    "DPONE_STUDIO_JSON_INVALID",
                    "Studio request body must be a valid JSON object.",
                    stage="studio_http",
                ) from exc
            if not isinstance(payload, Mapping):
                raise StudioError(
                    "DPONE_STUDIO_REQUEST_SCHEMA_INVALID",
                    "Studio request body must be a JSON object.",
                    stage="studio_http",
                )
            return payload

        def _send_error(
            self,
            error: StudioError,
            *,
            actor: str = "anonymous",
        ) -> None:
            path = urlparse(getattr(self, "path", "") or "").path
            self._send_json(
                error.to_payload(),
                status=getattr(error, "http_status", studio_error_status(error.code)),
                actor=actor,
                allowed=_safe_allowed_methods(router, path),
            )

        def _send_json(
            self,
            response: Mapping[str, Any],
            *,
            status: int,
            actor: str,
            allowed: tuple[str, ...] = (),
        ) -> None:
            body = json.dumps(
                response,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ).encode("utf-8")
            path = urlparse(getattr(self, "path", "") or "").path
            method = request_method(self)
            audit.record(
                method=method,
                path=_safe_audit_path(router, method, path),
                status=status,
                actor=actor,
            )
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            if _safe_is_deprecated(router, method, path):
                self.send_header("Deprecation", "true")
                self.send_header("Sunset", "Fri, 23 Jul 2027 00:00:00 GMT")
                self.send_header(
                    "Link",
                    '</openapi.json>; rel="deprecation"; type="application/json"',
                )
            headers = getattr(self, "headers", None)
            origin = headers.get("Origin") if headers is not None else None
            response_origin = config.response_origin(
                origin,
                server_authority(self.server.server_address),
            )
            if response_origin is not None:
                self.send_header("Access-Control-Allow-Origin", response_origin)
                self.send_header("Vary", "Origin")
            if allowed:
                rendered = ", ".join((*allowed, "OPTIONS"))
                self.send_header("Allow", rendered)
                self.send_header("Access-Control-Allow-Methods", rendered)
                self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
            self.end_headers()
            if method != "HEAD":
                self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002,N802
            return None

    return Handler


def _safe_allowed_methods(
    router: StudioRequestRouter,
    path: str,
) -> tuple[str, ...]:
    try:
        return router.allowed_methods(path)
    except StudioError:
        return ()


def _safe_audit_path(
    router: StudioRequestRouter,
    method: str,
    path: str,
) -> str:
    try:
        return router.audit_path(method, path)
    except StudioError:
        return "/<invalid>"


def _safe_is_deprecated(
    router: StudioRequestRouter,
    method: str,
    path: str,
) -> bool:
    try:
        return router.is_deprecated(method, path)
    except StudioError:
        return False


__all__ = ["StudioThreadingHTTPServer", "build_studio_http_handler"]
