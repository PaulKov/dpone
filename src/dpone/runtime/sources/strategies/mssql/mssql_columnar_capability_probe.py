"""MSSQL columnar source-capability probe adapter."""

from __future__ import annotations

from typing import Any

from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotCapability
from dpone.runtime.sources.strategies.mssql import mssql_columnar_range_admission

_SECRET_MARKERS = ("password=", "pwd=", "secret=", "token=", "access_key=")


class MssqlColumnarSourceCapabilityProbe:
    """Adapt request construction failures into redacted capability evidence."""

    def __init__(self, request_factory: Any, provider: Any) -> None:
        self._request_factory = request_factory
        self._provider = provider

    def __call__(self, **context: Any) -> ColumnarSnapshotCapability:
        try:
            request = self._request_factory(
                load_config=context["load_config"],
                source=context["source"],
                sink=context["sink"],
                state=None,
                load_record=context.get("load_record"),
            )
        except Exception as exc:
            message = _redact_exception_message(exc)
            blocker = f"source.columnar_request_failed:{type(exc).__name__}"
            if message in mssql_columnar_range_admission.RANGE_TERMINAL_BLOCKERS:
                blocker = message
            return ColumnarSnapshotCapability(
                provider_id=getattr(self._provider, "provider_id", "columnar_snapshot_provider"),
                certified=False,
                blockers=(blocker,),
                details={"exception": type(exc).__name__, "message": message},
            )
        return self._provider.capabilities(request)


def _redact_exception_message(exc: Exception) -> str:
    message = str(exc)
    if not message:
        return ""
    lowered = message.lower()
    if any(marker in lowered for marker in _SECRET_MARKERS):
        return "***REDACTED***"
    return message[:500]


__all__ = ["MssqlColumnarSourceCapabilityProbe"]
