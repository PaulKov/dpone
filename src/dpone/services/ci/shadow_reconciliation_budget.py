"""Parent-capped request, response-byte, and wall-time accounting."""

from __future__ import annotations

from collections.abc import Callable
from time import monotonic
from typing import Final, Literal, TypeVar

from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicy

RequestClass = Literal[
    "producer_list_page_requests",
    "auditor_list_page_requests",
    "exact_producer_run_requests",
    "attempt_run_requests",
    "jobs_page_requests",
    "artifact_metadata_requests",
    "artifact_download_requests",
    "redirect_requests",
    "git_object_requests",
    "pull_request_identity_requests",
    "polling_requests",
]
ResponseClass = Literal["api_response_body_bytes", "artifact_download_body_bytes"]
_Response = TypeVar("_Response")

_REQUEST_CLASSES: Final[tuple[RequestClass, ...]] = (
    "producer_list_page_requests",
    "auditor_list_page_requests",
    "exact_producer_run_requests",
    "attempt_run_requests",
    "jobs_page_requests",
    "artifact_metadata_requests",
    "artifact_download_requests",
    "redirect_requests",
    "git_object_requests",
    "pull_request_identity_requests",
    "polling_requests",
)


class ResourceLimitExceeded(RuntimeError):
    """One parent-owned hard resource maximum was crossed."""


class RequestBudget:
    """Meter every dispatch before transport and saturate evidence at hard limits."""

    def __init__(self, *, policy: ReconciliationPolicy, monotonic_clock: Callable[[], float] = monotonic) -> None:
        self._policy = policy
        self._clock = monotonic_clock
        self._started_at = monotonic_clock()
        self._request_counters: dict[RequestClass, int] = {name: 0 for name in _REQUEST_CLASSES}
        self._api_response_bytes = 0
        self._artifact_download_bytes = 0
        self._producer_runs = 0
        self._auditor_runs = 0
        self._producer_attempts = 0
        self._retry_requests = 0
        self._limits_crossed: set[str] = set()

    @property
    def limits_crossed(self) -> tuple[str, ...]:
        """Return parent hard maxima in canonical order."""

        order = ("total_http_requests", "total_response_body_bytes", "execution_wall_seconds")
        return tuple(name for name in order if name in self._limits_crossed)

    @property
    def remaining_response_bytes(self) -> int:
        """Return the only safe maximum raw response read for the next dispatch."""

        return self._policy.hard_max_response_bytes - self._api_response_bytes - self._artifact_download_bytes

    def dispatch(
        self, request_class: RequestClass, operation: Callable[[float], bytes], *, retry: bool = False
    ) -> bytes:
        """Count one outbound operation before invoking it with remaining wall time."""

        timeout = self._reserve_dispatch(request_class, retry=retry)
        payload = operation(timeout)
        self.record_response_bytes("api_response_body_bytes", len(payload))
        return payload

    def dispatch_response(
        self,
        request_class: RequestClass,
        operation: Callable[[float], tuple[_Response, bytes]],
        *,
        response_class: ResponseClass,
        retry: bool = False,
    ) -> _Response:
        """Meter a non-JSON response while retaining trusted response metadata."""

        timeout = self._reserve_dispatch(request_class, retry=retry)
        response, raw_bytes = operation(timeout)
        self.record_response_bytes(response_class, len(raw_bytes))
        return response

    def record_response_bytes(self, response_class: ResponseClass, count: int) -> None:
        """Add application-delivered bytes and saturate the aggregate at the cap."""

        if count < 0:
            raise ValueError("response byte count cannot be negative")
        if response_class == "api_response_body_bytes":
            self._api_response_bytes += count
        else:
            self._artifact_download_bytes += count
        total = self._api_response_bytes + self._artifact_download_bytes
        if total > self._policy.hard_max_response_bytes:
            excess = total - self._policy.hard_max_response_bytes
            if response_class == "api_response_body_bytes":
                self._api_response_bytes -= excess
            else:
                self._artifact_download_bytes -= excess
            self._limits_crossed.add("total_response_body_bytes")
            raise ResourceLimitExceeded("total_response_body_bytes hard maximum crossed")

    def counters(self) -> dict[str, int | float]:
        """Return closed counters suitable for a capacity evidence payload."""

        elapsed = self._elapsed_or_limit()
        total_response = self._api_response_bytes + self._artifact_download_bytes
        total_requests = sum(self._request_counters.values())
        result: dict[str, int | float] = {
            "producer_runs": self._producer_runs,
            "producer_attempts": self._producer_attempts,
            "auditor_runs": self._auditor_runs,
            "retry_requests": self._retry_requests,
            "api_response_body_bytes": self._api_response_bytes,
            "artifact_download_body_bytes": self._artifact_download_bytes,
            "total_response_body_bytes": total_response,
            "total_http_requests": total_requests,
            "execution_wall_seconds": elapsed,
        }
        result.update({str(name): count for name, count in self._request_counters.items()})
        return result

    def record_observed_runs(self, *, producer: int = 0, auditor: int = 0, attempts: int = 0) -> None:
        """Record bounded provider objects after their complete acquisition."""

        values = (producer, auditor, attempts)
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values):
            raise ValueError("observed object counts must be non-negative integers")
        # These counts are evidence metadata, not independent resource limits.
        # Retain them beside dispatch counters without letting a caller reset them.
        self._producer_runs += producer
        self._auditor_runs += auditor
        self._producer_attempts += attempts

    def _reserve_dispatch(self, request_class: RequestClass, *, retry: bool) -> float:
        elapsed = self._elapsed_or_limit()
        if elapsed >= self._policy.hard_max_wall_seconds:
            self._limits_crossed.add("execution_wall_seconds")
            raise ResourceLimitExceeded("execution_wall_seconds hard maximum crossed")
        total_requests = sum(self._request_counters.values())
        if total_requests >= self._policy.hard_max_http_requests:
            self._limits_crossed.add("total_http_requests")
            raise ResourceLimitExceeded("total_http_requests hard maximum crossed")
        if self.remaining_response_bytes <= 0:
            self._limits_crossed.add("total_response_body_bytes")
            raise ResourceLimitExceeded("total_response_body_bytes hard maximum crossed")
        self._request_counters[request_class] += 1
        if retry:
            self._retry_requests += 1
        return self._policy.hard_max_wall_seconds - elapsed

    def _elapsed_or_limit(self) -> float:
        elapsed = self._clock() - self._started_at
        if elapsed < 0:
            raise ResourceLimitExceeded("monotonic clock moved backwards")
        if elapsed > self._policy.hard_max_wall_seconds:
            self._limits_crossed.add("execution_wall_seconds")
            return float(self._policy.hard_max_wall_seconds)
        return elapsed


__all__ = ["RequestBudget", "RequestClass", "ResourceLimitExceeded", "ResponseClass"]
