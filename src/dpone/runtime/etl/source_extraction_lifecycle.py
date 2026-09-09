"""Reusable source-extraction lifecycle adapter.

Sources with a vendor-native snapshot authority continue to issue their own
receipt.  Every other source is wrapped at the orchestration boundary so eager
exports receive a truthful orchestration read-window boundary and lazy artifacts receive
one authority that they complete only at their real consumption boundary.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import is_dataclass, replace
from datetime import datetime, timezone
from typing import Any, cast

from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority

_EAGER = "eager"
_STREAM_ACQUIRED = "stream_acquired"
_LAZY = "lazy"
_SUPPORTED_MODES = frozenset({_EAGER, _STREAM_ACQUIRED, _LAZY})
_UTC = timezone.utc  # noqa: UP017 - project mypy uses Python 3.10 datetime stubs.


class SourceExtractionLifecycleError(RuntimeError):
    """A source/artifact cannot publish truthful extraction evidence."""


class SourceExtractionLifecycleService:
    """Attach one truthful lifecycle authority to every extraction result."""

    def __init__(self, *, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(_UTC))

    def assert_supported(self, source: object, load_config: object) -> None:
        """Fail before source reads when a source explicitly rejects the contract."""

        capability = getattr(source, "extraction_lifecycle_capability", None)
        if not callable(capability):
            return
        value = capability(load_config)
        if value is False or str(value).strip().lower() in {"unsupported", "unprovable"}:
            raise SourceExtractionLifecycleError("source_extraction_lifecycle.unsupported")

    def capture(self, extract: Callable[[], Any]) -> Any:
        """Run extraction and attach timing without replacing native authority."""

        invocation_boundary = self._clock()
        result = extract()
        native = getattr(result, "extraction_lifecycle", None)
        if isinstance(native, ExtractionLifecycleAuthority):
            return result

        artifact = getattr(result, "artifact", None)
        mode = str(getattr(artifact, "extraction_completion_mode", _EAGER)).strip().lower()
        if mode not in _SUPPORTED_MODES:
            raise SourceExtractionLifecycleError(f"source_extraction_lifecycle.unsupported_artifact_mode:{mode}")

        authority = ExtractionLifecycleAuthority(clock=self._clock)
        if mode in {_EAGER, _STREAM_ACQUIRED}:
            authority.acquire(started_at=invocation_boundary)
        if mode == _EAGER:
            authority.complete()
        bind = getattr(artifact, "bind_extraction_lifecycle", None)
        if callable(bind):
            bind(authority)
        elif mode != _EAGER:
            raise SourceExtractionLifecycleError("source_extraction_lifecycle.lazy_artifact_binding_required")
        return _replace_lifecycle(result, authority)


def _replace_lifecycle(result: Any, authority: ExtractionLifecycleAuthority) -> Any:
    if is_dataclass(result):
        return replace(cast(Any, result), extraction_lifecycle=authority)
    try:
        setattr(result, "extraction_lifecycle", authority)
    except (AttributeError, TypeError) as exc:
        raise SourceExtractionLifecycleError("source_extraction_lifecycle.result_binding_required") from exc
    return result


__all__ = ["SourceExtractionLifecycleError", "SourceExtractionLifecycleService"]
