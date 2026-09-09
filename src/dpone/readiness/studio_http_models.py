"""HTTP-boundary value objects for the local Studio adapter."""

from __future__ import annotations

import hmac
import ipaddress
import threading
from collections import deque
from dataclasses import dataclass
from time import time
from typing import Any
from urllib.parse import urlparse

from dpone.readiness.studio_errors import StudioError

# Temporary compatibility export for internal callers and tests migrating from
# the pre-v1 HTTP-coupled application exception.
StudioApiError = StudioError


@dataclass(frozen=True, slots=True)
class StudioHttpConfig:
    host: str = "127.0.0.1"
    token: str = ""
    allow_remote: bool = False
    cors_origins: tuple[str, ...] = ()
    body_limit_bytes: int = 1024 * 1024
    request_timeout_seconds: float = 10.0
    max_concurrent_requests: int = 32
    audit_capacity: int = 1000

    def validate(self) -> None:
        remote = not _is_loopback_host(self.host)
        if remote and not self.allow_remote:
            raise StudioError(
                "DPONE_STUDIO_REMOTE_NOT_ALLOWED",
                "Non-loopback Studio binding requires --allow-remote.",
                stage="studio_startup",
            )
        if remote and not self.token:
            raise StudioError(
                "DPONE_STUDIO_REMOTE_TOKEN_REQUIRED",
                "Remote Studio binding requires DPONE_STUDIO_TOKEN.",
                stage="studio_startup",
            )
        if remote and not self.cors_origins:
            raise StudioError(
                "DPONE_STUDIO_REMOTE_CORS_REQUIRED",
                "Remote Studio binding requires at least one exact CORS origin.",
                stage="studio_startup",
            )
        for origin in self.cors_origins:
            parsed = urlparse(origin)
            if (
                _contains_control_characters(origin)
                or parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.path not in {"", "/"}
                or parsed.query
                or parsed.fragment
                or parsed.username is not None
                or parsed.password is not None
            ):
                raise StudioError(
                    "DPONE_STUDIO_CORS_ORIGIN_INVALID",
                    "Studio CORS origins must be exact http(s) origins without paths.",
                    stage="studio_startup",
                )

    def request_host_allowed(self, request_host: str, trusted_authority: str) -> bool:
        if self.allow_remote and not _is_loopback_host(self.host):
            return True
        return request_host == trusted_authority or _same_loopback_authority(
            request_host,
            trusted_authority,
        )

    def origin_allowed(self, origin: str | None, trusted_authority: str) -> bool:
        if not origin:
            return True
        if self.cors_origins:
            return origin in self.cors_origins
        parsed = urlparse(origin)
        return (
            parsed.scheme in {"http", "https"}
            and parsed.netloc == trusted_authority
            and parsed.hostname is not None
            and _is_loopback_host(parsed.hostname)
        )

    def response_origin(self, origin: str | None, trusted_authority: str) -> str | None:
        """Return a trusted CORS header value without reflecting request input."""

        if not origin or not self.origin_allowed(origin, trusted_authority):
            return None
        for configured_origin in self.cors_origins:
            if hmac.compare_digest(origin, configured_origin):
                return configured_origin
        scheme = "https" if urlparse(origin).scheme == "https" else "http"
        return f"{scheme}://{trusted_authority}"

    def token_matches(self, candidate: str) -> bool:
        return bool(self.token) and hmac.compare_digest(candidate, self.token)


class StudioAuditLog:
    """Thread-safe bounded in-memory audit log with monotonic local ordering."""

    def __init__(self, *, capacity: int = 1000) -> None:
        self._events: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._sequence = 0
        self._capacity = capacity

    def record(self, *, method: str, path: str, status: int, actor: str) -> None:
        with self._lock:
            self._sequence += 1
            self._events.append(
                {
                    "sequence": self._sequence,
                    "ts": time(),
                    "actor": actor,
                    "method": method,
                    "path": path,
                    "status": status,
                }
            )

    def snapshot(self, *, limit: int) -> dict[str, Any]:
        if not 1 <= limit <= 200:
            raise StudioError(
                "DPONE_STUDIO_PAGE_LIMIT_INVALID",
                "Pagination limit must be between 1 and 200.",
            )
        with self._lock:
            events = list(self._events)[-limit:]
        return {
            "events": events,
            "capacity": self._capacity,
            "ordering": "sequence",
        }


def _is_loopback_host(host: str) -> bool:
    normalized = host.strip().lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _contains_control_characters(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _same_loopback_authority(candidate: str, trusted: str) -> bool:
    candidate_url = urlparse(f"//{candidate}")
    trusted_url = urlparse(f"//{trusted}")
    try:
        return (
            candidate_url.hostname is not None
            and trusted_url.hostname is not None
            and _is_loopback_host(candidate_url.hostname)
            and _is_loopback_host(trusted_url.hostname)
            and candidate_url.port == trusted_url.port
        )
    except ValueError:
        return False


__all__ = ["StudioApiError", "StudioAuditLog", "StudioHttpConfig"]
