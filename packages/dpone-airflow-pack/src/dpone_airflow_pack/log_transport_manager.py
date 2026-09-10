"""Explicit, execution-scoped provenance for provider log transport failures.

The operator owns the boundary and injects it into each manager generation.
Credential refresh therefore preserves classification without modifying provider
methods. Context-local activation also keeps concurrent executions isolated.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

_TRANSIENT_KUBERNETES_API_STATUSES = frozenset({429, 500, 502, 503, 504})


class RetryableLiveLogTransportError(RuntimeError):
    """Private proof that a retryable error came from a manager's log reader."""

    def __init__(self, status: int, pod_manager: Any) -> None:
        super().__init__("retryable Kubernetes live-log transport failure")
        self.status = status
        self.pod_manager = pod_manager


class LiveLogTransportBoundary:
    """Operator-owned classifier shared explicitly with refreshed managers."""

    def __init__(self) -> None:
        self._exception_types: ContextVar[tuple[type[BaseException], ...]] = ContextVar(
            "live_log_exception_types", default=()
        )

    def __reduce__(self) -> tuple[type[LiveLogTransportBoundary], tuple[()]]:
        """Copy task graphs with a fresh inactive scope, never a live context.

        Pickle/deepcopy memoization preserves shared references between the
        copied operator and its managers while isolating them from the original.
        """
        return type(self), ()

    @contextmanager
    def classify(self, exception_types: tuple[type[BaseException], ...]) -> Iterator[None]:
        """Activate classification only for this completion attempt and context."""
        token = self._exception_types.set(exception_types)
        try:
            yield
        finally:
            self._exception_types.reset(token)

    def read(self, manager: Any, reader: Callable[[], Any]) -> Any:
        """Keep untyped, non-transient and out-of-scope failures unchanged."""
        try:
            return reader()
        except Exception as exc:
            status = _bounded_http_status(getattr(exc, "status", None))
            if isinstance(exc, self._exception_types.get()) and status in _TRANSIENT_KUBERNETES_API_STATUSES:
                raise RetryableLiveLogTransportError(status, manager) from None
            raise


class LogTransportPodManagerMixin:
    """Intercept internal provider dispatch through ordinary inheritance.

    Compose before the provider PodManager in the MRO. A proxy around a manager
    would miss its internal ``self.read_pod_logs`` calls. All other provider
    operations and constructor arguments retain their native implementation.
    """

    def __init__(self, *, log_transport_boundary: LiveLogTransportBoundary, **kwargs: Any) -> None:
        self.log_transport_boundary = log_transport_boundary
        super().__init__(**kwargs)

    def read_pod_logs(self, *args: Any, **kwargs: Any) -> Any:
        reader = super().read_pod_logs  # type: ignore[misc]
        return self.log_transport_boundary.read(self, lambda: reader(*args, **kwargs))


def _bounded_http_status(value: object) -> int | None:
    """Return one bounded HTTP status without accepting booleans."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        status = value
    elif isinstance(value, str) and len(value) == 3 and value.isascii() and value.isdigit():
        status = int(value)
    else:
        return None
    return status if 100 <= status <= 599 else None
