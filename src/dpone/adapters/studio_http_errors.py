"""HTTP-specific error mapping for the local Studio adapter."""

from __future__ import annotations

from dpone.readiness.studio_errors import StudioError


class StudioHttpError(StudioError):
    """Transport-owned error carrying only the adapter's HTTP status."""

    def __init__(self, code: str, message: str, *, status: int) -> None:
        super().__init__(code, message, stage="studio_http")
        self.http_status = status


def http_error(code: str, status: int) -> StudioError:
    messages = {
        "DPONE_STUDIO_CONCURRENCY_LIMIT": "Studio request concurrency limit was reached.",
        "DPONE_STUDIO_REQUEST_TIMEOUT": "Studio request timed out.",
        "DPONE_STUDIO_INTERNAL_ERROR": "Studio could not complete the request safely.",
    }
    return StudioHttpError(code, messages[code], status=status)


def studio_error_status(code: str) -> int:
    exact = {
        "DPONE_STUDIO_JSON_INVALID": 400,
        "DPONE_STUDIO_CONTENT_LENGTH_INVALID": 400,
        "DPONE_STUDIO_HTTP_REQUEST_INVALID": 400,
        "DPONE_STUDIO_UNAUTHORIZED": 401,
        "DPONE_STUDIO_HOST_FORBIDDEN": 403,
        "DPONE_STUDIO_ORIGIN_FORBIDDEN": 403,
        "DPONE_STUDIO_PATH_UNSAFE": 403,
        "DPONE_STUDIO_ROUTE_NOT_FOUND": 404,
        "DPONE_STUDIO_ROUTE_INVALID": 404,
        "DPONE_ROUTE_NOT_SUPPORTED": 404,
        "DPONE_PIPELINE_SOURCE_NOT_FOUND": 404,
        "DPONE_STUDIO_METHOD_NOT_ALLOWED": 405,
        "DPONE_STUDIO_REQUEST_TIMEOUT": 408,
        "DPONE_STUDIO_BODY_TOO_LARGE": 413,
        "DPONE_STUDIO_MANIFEST_TOO_LARGE": 413,
        "DPONE_STUDIO_MEDIA_TYPE_UNSUPPORTED": 415,
        "DPONE_STUDIO_CONCURRENCY_LIMIT": 429,
        "DPONE_STUDIO_INTERNAL_ERROR": 500,
        "DPONE_STUDIO_ROUTE_NOT_IMPLEMENTED": 500,
        "DPONE_STUDIO_BIND_FAILED": 503,
    }
    return exact.get(code, 422)


__all__ = [
    "StudioError",
    "StudioHttpError",
    "http_error",
    "studio_error_status",
]
