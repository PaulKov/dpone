"""Bounded high-confidence secret scan for immutable dbt source bundles."""

from __future__ import annotations

import re

_OVERLAP_BYTES = 4096
_SECRET_RE = re.compile(
    rb"""(?ix)
    -----BEGIN(?:\ [A-Z0-9]+)*\ PRIVATE\ KEY
    |
    [a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@
    |
    (?:password|passwd|pwd|api[_-]?key|access[_-]?key|secret[_-]?key|
       client[_-]?secret|session[_-]?token|authorization|connection[_-]?string)
    \s*[:=]\s*
    (?!\{\{|\$\{|env:|vault:|secret:|\[redacted\]|<redacted>|\*{3,})
    (?:"[^"\r\n]+"|'[^'\r\n]+'|[^\s,;&#}\]\r\n]{8,})
    """,
)


class DbtProjectBundleSecretDetected(ValueError):
    """A secret-like value was found without retaining or exposing its bytes."""


class DbtProjectBundleSecretScanner:
    """Reject secret-like source bytes without retaining their values."""

    def __init__(self) -> None:
        self._tail = b""

    def feed(self, chunk: bytes) -> None:
        candidate = self._tail + chunk
        if _SECRET_RE.search(candidate):
            raise DbtProjectBundleSecretDetected
        self._tail = candidate[-_OVERLAP_BYTES:]


__all__ = [
    "DbtProjectBundleSecretDetected",
    "DbtProjectBundleSecretScanner",
]
