"""Read-only source bindings for repeated target-cursor route admission.

A binding supplies the current facade values, never a cached admission result.
These structural interfaces issue no permission and perform no connector I/O.
"""

from typing import Protocol


class SourceCursorSinkBinding(Protocol):
    """The caller's sink identity, which may differ from a strategy fallback."""

    @property
    def sink_connector(self) -> object: ...


class SourceCursorRouteBinding(SourceCursorSinkBinding, Protocol):
    """Explicit source policy identity in addition to the caller's sink."""

    @property
    def target_max_cursor_source_type(self) -> str: ...
